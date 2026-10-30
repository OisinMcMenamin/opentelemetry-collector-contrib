// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsattributelimitprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/awsattributelimitprocessor"

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// Attribute source constants used for classification and switch dispatch.
const (
	attrSourceResource  = "resource"
	attrSourceScope     = "scope"
	attrSourceDatapoint = "datapoint"
)

// attributeLimitProcessor enforces the aws backend attribute limit by removing
// redundant attributes and dropping low-priority attributes
// by tier when the total count exceeds the configured maximum.
type attributeLimitProcessor struct {
	config                       *Config
	logger                       *zap.Logger
	unconditionalRemovalKeys     map[string]struct{}
	unconditionalRemovalPrefixes []string
	mu                           sync.Mutex
	lastLogAt                    map[string]time.Time // rate-limiting: last log time per metric name
	lastEviction                 time.Time
}

func newProcessor(cfg *Config, logger *zap.Logger) *attributeLimitProcessor {
	keySet := make(map[string]struct{}, len(cfg.UnconditionalRemovalKeys))
	for _, k := range cfg.UnconditionalRemovalKeys {
		keySet[k] = struct{}{}
	}
	return &attributeLimitProcessor{
		config:                       cfg,
		logger:                       logger,
		unconditionalRemovalKeys:     keySet,
		unconditionalRemovalPrefixes: cfg.UnconditionalRemovalPrefixes,
		lastLogAt:                    make(map[string]time.Time),
		lastEviction:                 time.Now(),
	}
}

// Start is a no-op for this processor (no external resources to initialize).
func (*attributeLimitProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

// Shutdown is a no-op for this processor (no external resources to release).
func (*attributeLimitProcessor) Shutdown(_ context.Context) error {
	return nil
}

// removeUnconditionalAttributes removes all resource attributes that match
// unconditional removal rules (prefix patterns and exact keys) in a single pass.
func (p *attributeLimitProcessor) removeUnconditionalAttributes(attrs pcommon.Map) {
	attrs.RemoveIf(func(key string, _ pcommon.Value) bool {
		// Check exact key match first (O(1) map lookup).
		if _, ok := p.unconditionalRemovalKeys[key]; ok {
			return true
		}
		// Check prefix patterns.
		for _, prefix := range p.unconditionalRemovalPrefixes {
			if strings.HasPrefix(key, prefix) {
				return true
			}
		}
		return false
	})
}

// attrEntry represents a droppable attribute with its tier and source.
type attrEntry struct {
	key    string
	tier   int
	source string // "datapoint", "scope", or "resource"
}

// attrEntryPool reduces allocations when Phase 2 runs frequently.
// We store *[]attrEntry (pointer to slice) rather than []attrEntry because
// sync.Pool stores interface{} values — storing the slice directly would cause
// the slice header to be boxed into an interface on every Get/Put, defeating
// the purpose. Storing a pointer avoids this allocation.
var attrEntryPool = sync.Pool{
	New: func() any {
		s := make([]attrEntry, 0, 64)
		return &s
	},
}

// removeExcessByTier collects droppable attributes from the leaf dimension, scope, and resource,
// sorts by tier then alphabetically, and removes them until the excess count is satisfied.
// Returns the number of attributes dropped and the min/max tier used.
//
// It is generic over the concrete leaf type so the metric datapoint path stays
// allocation-free: a concrete leaf is not boxed into an interface per record.
func removeExcessByTier[L leafAttributes](resourceAttrs, scopeAttrs pcommon.Map, leaf L, excess int) (droppedCount, minTier, maxTier int) {
	droppablePtr := attrEntryPool.Get().(*[]attrEntry)
	droppable := (*droppablePtr)[:0]
	defer func() {
		*droppablePtr = droppable[:0]
		attrEntryPool.Put(droppablePtr)
	}()

	// Scan resource attributes (tiers 1-8).
	resourceAttrs.Range(func(key string, _ pcommon.Value) bool {
		tier := classifyAttribute(key, attrSourceResource)
		if tier > 0 {
			droppable = append(droppable, attrEntry{key: key, tier: tier, source: attrSourceResource})
		}
		return true
	})

	// Scan scope attributes (tier 9).
	scopeAttrs.Range(func(key string, _ pcommon.Value) bool {
		tier := classifyAttribute(key, attrSourceScope)
		if tier > 0 {
			droppable = append(droppable, attrEntry{key: key, tier: tier, source: attrSourceScope})
		}
		return true
	})

	// Scan leaf attributes -- metric datapoint attributes or profile attributes (tier 10).
	leaf.rangeKeys(func(key string) bool {
		tier := classifyAttribute(key, attrSourceDatapoint)
		if tier > 0 {
			droppable = append(droppable, attrEntry{key: key, tier: tier, source: attrSourceDatapoint})
		}
		return true
	})

	// Sort by tier ascending, then alphabetically within tier.
	slices.SortFunc(droppable, func(a, b attrEntry) int {
		if a.tier != b.tier {
			return a.tier - b.tier
		}
		return strings.Compare(a.key, b.key)
	})

	// Drop until excess is satisfied.
	dropped := 0
	minT, maxT := 0, 0
	for _, entry := range droppable {
		if dropped >= excess {
			break
		}
		switch entry.source {
		case attrSourceDatapoint:
			leaf.remove(entry.key)
		case attrSourceScope:
			scopeAttrs.Remove(entry.key)
		case attrSourceResource:
			resourceAttrs.Remove(entry.key)
		}
		dropped++
		if minT == 0 {
			minT = entry.tier
		}
		maxT = entry.tier
	}

	return dropped, minT, maxT
}

// logDropWarning emits a rate-limited warning when tier-based dropping occurs.
// Logs at most once per subject per minute. subjectField is the zap field key
// ("metric" for metrics, "profile" for profiles) so each signal keeps its own
// log schema; the metrics path is unchanged from before profiles support.
func (p *attributeLimitProcessor) logDropWarning(subjectField, subject string, droppedCount, minTier, maxTier int) {
	if !p.shouldLog(subject) {
		return
	}
	p.logger.Warn("dropped attributes to meet limit",
		zap.String(subjectField, subject),
		zap.Int("dropped", droppedCount),
		zap.Int("minTier", minTier),
		zap.Int("maxTier", maxTier),
		zap.Int("limit", p.config.MaxTotalAttributes),
	)
}

// logExhaustedError logs a rate-limited error when all tiers are exhausted
// and force-pruning was required to meet the limit.
func (p *attributeLimitProcessor) logExhaustedError(subjectField, subject string, remaining, forcePruned int) {
	errorKey := subject + ":exhausted"
	if !p.shouldLog(errorKey) {
		return
	}
	p.logger.Error("all tiers exhausted, force-pruned protected attributes to meet limit",
		zap.String(subjectField, subject),
		zap.Int("attributesBeforePrune", remaining),
		zap.Int("forcePruned", forcePruned),
		zap.Int("limit", p.config.MaxTotalAttributes),
	)
}

// shouldLog checks rate limiting and evicts stale entries. Returns true if
// the caller should proceed with logging. Releases the mutex before returning.
func (p *attributeLimitProcessor) shouldLog(key string) bool {
	p.mu.Lock()
	now := time.Now()
	if now.Sub(p.lastEviction) > 5*time.Minute {
		for name, lastTime := range p.lastLogAt {
			if now.Sub(lastTime) > 5*time.Minute {
				delete(p.lastLogAt, name)
			}
		}
		p.lastEviction = now
	}
	if lastTime, ok := p.lastLogAt[key]; ok && now.Sub(lastTime) < time.Minute {
		p.mu.Unlock()
		return false
	}
	p.lastLogAt[key] = now
	p.mu.Unlock()
	return true
}

// enforceLimit checks if the total attribute count exceeds the limit and runs
// Phase 2 tier-based dropping if needed. The leaf is the innermost per-record
// attribute dimension: metric datapoint attributes or a profile's attributes.
//
// It is a generic free function (not a method) because Go methods cannot take
// type parameters; keeping it generic over the concrete leaf type avoids boxing
// the leaf into an interface on the metrics hot path.
func enforceLimit[L leafAttributes](p *attributeLimitProcessor, resourceAttrs, scopeAttrs pcommon.Map, leaf L, subjectField, subject string) {
	total := resourceAttrs.Len() + scopeAttrs.Len() + leaf.Len()
	if total <= p.config.MaxTotalAttributes {
		return
	}

	excess := total - p.config.MaxTotalAttributes
	droppedCount, minTier, maxTier := removeExcessByTier(resourceAttrs, scopeAttrs, leaf, excess)

	if droppedCount > 0 {
		p.logDropWarning(subjectField, subject, droppedCount, minTier, maxTier)
	}

	// If still over limit after tier-based dropping, force-prune remaining
	// attributes (including protected ones) to guarantee we never exceed the limit.
	remaining := resourceAttrs.Len() + scopeAttrs.Len() + leaf.Len()
	if remaining > p.config.MaxTotalAttributes {
		forcePruned := forcePrune(p, resourceAttrs, scopeAttrs, leaf)
		p.logExhaustedError(subjectField, subject, remaining, forcePruned)
	}
}

// forcePrune is a last-resort safety net that should never activate in practice.
// It runs only after all tier-based dropping is exhausted, i.e. only
// tier-undroppable attributes remain, and guarantees the hard limit is never
// exceeded.
//
// It drops in two passes. Pass 1 drops NON-identity attributes in blast-radius
// order (leaf first, then scope, then resource), keeping identity: service.name,
// service.instance.id, k8s pod identity (k8s.pod.name/namespace.name/uid),
// cloud.resource_id, and the agent's cloudwatch.* scope stamps. This matters for
// a resource-heavy producer whose attributes are all tier-undroppable: without
// it, resource attributes are removed alphabetically from the end and
// service.name (which sorts late) is dropped first, so the backend rejects the
// record for a missing service.name -- the very outcome the processor exists to
// prevent. Pass 2 is the true last resort: if the record carries more identity
// attributes than the limit (a misconfiguration), it drops identity too,
// alphabetically from the end, to still guarantee the limit.
//
// This applies to both metrics and profiles (shared code). It changes only which
// attributes survive the last-resort path, never the resulting count, and
// force-prune is a should-never-fire safety net, so the metrics change is inert
// in normal operation and strictly protects identity when it does fire.
//
// Generic over the concrete leaf type for the same no-boxing reason as enforceLimit.
func forcePrune[L leafAttributes](p *attributeLimitProcessor, resourceAttrs, scopeAttrs pcommon.Map, leaf L) int {
	excess := resourceAttrs.Len() + scopeAttrs.Len() + leaf.Len() - p.config.MaxTotalAttributes
	if excess <= 0 {
		return 0
	}

	pruned := 0

	// Pass 1: drop non-identity, leaf -> scope -> resource. Leaf carries no
	// identity, so it is dropped freely and first.
	pruned += pruneFromLeaf(leaf, excess-pruned)
	if pruned < excess {
		pruned += pruneFromMap(scopeAttrs, excess-pruned, isForcePruneProtectedScope)
	}
	if pruned < excess {
		pruned += pruneFromMap(resourceAttrs, excess-pruned, isForcePruneProtectedResource)
	}

	// Pass 2 (last resort): more identity than the limit; drop it too.
	if pruned < excess {
		pruned += pruneFromMap(scopeAttrs, excess-pruned, nil)
	}
	if pruned < excess {
		pruned += pruneFromMap(resourceAttrs, excess-pruned, nil)
	}

	return pruned
}

// pruneFromLeaf removes up to `count` attributes from the leaf dimension, sorted
// alphabetically (removes last keys first). Returns the number actually removed.
func pruneFromLeaf[L leafAttributes](leaf L, count int) int {
	if count <= 0 || leaf.Len() == 0 {
		return 0
	}

	keys := make([]string, 0, leaf.Len())
	leaf.rangeKeys(func(key string) bool {
		keys = append(keys, key)
		return true
	})
	slices.Sort(keys)

	// Remove from the end (alphabetically last). Each remove drops one occurrence.
	removed := 0
	for i := len(keys) - 1; i >= 0 && removed < count; i-- {
		leaf.remove(keys[i])
		removed++
	}
	return removed
}

// pruneFromMap removes up to `count` attributes from the map, sorted alphabetically
// (removes last keys first). When keep is non-nil, keys for which keep returns
// true are skipped (not removed). Returns the number actually removed.
func pruneFromMap(attrs pcommon.Map, count int, keep func(string) bool) int {
	if count <= 0 || attrs.Len() == 0 {
		return 0
	}

	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(key string, _ pcommon.Value) bool {
		keys = append(keys, key)
		return true
	})
	slices.Sort(keys)

	// Remove from the end (alphabetically last), skipping kept keys.
	removed := 0
	for i := len(keys) - 1; i >= 0 && removed < count; i-- {
		if keep != nil && keep(keys[i]) {
			continue
		}
		attrs.Remove(keys[i])
		removed++
	}
	return removed
}

// leafAttributes abstracts the innermost per-record attribute dimension so the
// tier-based limit algorithm is shared across signals: metric datapoint
// attributes (a pcommon.Map) and profile attributes (dictionary-encoded indices,
// see processor_profiles.go). Resource and scope attributes are always
// pcommon.Map and are handled directly.
type leafAttributes interface {
	// Len reports the number of attributes in the dimension.
	Len() int
	// rangeKeys calls f for each attribute key; it stops early if f returns false.
	rangeKeys(f func(key string) bool)
	// remove deletes a single attribute occurrence with the given key.
	remove(key string)
}

// mapLeaf adapts a pcommon.Map (metric datapoint attributes) to leafAttributes.
type mapLeaf struct{ attrs pcommon.Map }

func (l mapLeaf) Len() int { return l.attrs.Len() }

func (l mapLeaf) rangeKeys(f func(string) bool) {
	l.attrs.Range(func(k string, _ pcommon.Value) bool { return f(k) })
}

func (l mapLeaf) remove(key string) { l.attrs.Remove(key) }

// processDatapoints is a generic helper that enforces the attribute limit on
// all datapoints in a slice. It works with any datapoint type that exposes
// Attributes() pcommon.Map.
func processDatapoints[DP interface{ Attributes() pcommon.Map }](
	datapoints interface {
		Len() int
		At(int) DP
	},
	p *attributeLimitProcessor,
	resourceAttrs pcommon.Map,
	scopeAttrs pcommon.Map,
	metricName string,
) {
	for i := 0; i < datapoints.Len(); i++ {
		dp := datapoints.At(i)
		enforceLimit(p, resourceAttrs, scopeAttrs, mapLeaf{dp.Attributes()}, "metric", metricName)
	}
}

func (p *attributeLimitProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceAttrs := rm.Resource().Attributes()

		// Unconditional removal (always runs).
		p.removeUnconditionalAttributes(resourceAttrs)

		// Phase 2: Per-datapoint evaluation.
		// Note: Resource attributes are shared across all datapoints in a ResourceMetrics.
		// If an earlier datapoint triggers Phase 2 and drops resource attributes, subsequent
		// datapoints see the already-trimmed resource attributes. This is by design — if any
		// datapoint is over the limit, the shared resource attributes need trimming regardless.
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scopeAttrs := sm.Scope().Attributes()

			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				metricName := m.Name()

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					processDatapoints(m.Gauge().DataPoints(), p, resourceAttrs, scopeAttrs, metricName)
				case pmetric.MetricTypeSum:
					processDatapoints(m.Sum().DataPoints(), p, resourceAttrs, scopeAttrs, metricName)
				case pmetric.MetricTypeHistogram:
					processDatapoints(m.Histogram().DataPoints(), p, resourceAttrs, scopeAttrs, metricName)
				case pmetric.MetricTypeExponentialHistogram:
					processDatapoints(m.ExponentialHistogram().DataPoints(), p, resourceAttrs, scopeAttrs, metricName)
				case pmetric.MetricTypeSummary:
					processDatapoints(m.Summary().DataPoints(), p, resourceAttrs, scopeAttrs, metricName)
				default:
					// Skip metrics with unsupported or empty type without error.
				}
			}
		}
	}
	return md, nil
}
