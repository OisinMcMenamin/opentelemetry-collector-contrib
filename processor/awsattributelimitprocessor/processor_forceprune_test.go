// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsattributelimitprocessor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// resource-heavy producer whose attributes are all tier-undroppable (plain
// non-label resource attrs). The tier phase cannot drop any of them, so
// force-prune runs. It must keep the identity attributes rather than remove them
// alphabetically from the end (service.name sorts late and would otherwise go
// first, getting the record rejected for a missing service.name).
func TestForcePrune_KeepsResourceIdentity(t *testing.T) {
	resource := map[string]string{
		"service.name":        "svc",
		"service.instance.id": "i-1",
		"cloud.resource_id":   "arn:aws:...",
	}
	for i := range 170 {
		resource[fmt.Sprintf("parity.attr.%03d", i)] = "v" // tier-undroppable, non-identity
	}
	require.Len(t, resource, 173)

	md := newTestMetrics(resource, nil, nil)
	p := newProcessor(&Config{MaxTotalAttributes: 150}, zap.NewNop())
	_, err := p.processMetrics(t.Context(), md)
	require.NoError(t, err)

	attrs := getResourceAttrs(md)
	assert.Equal(t, 150, attrs.Len(), "trimmed to exactly the limit")
	for _, k := range []string{"service.name", "service.instance.id", "cloud.resource_id"} {
		_, ok := attrs.Get(k)
		assert.Truef(t, ok, "identity attr %q must survive force-prune", k)
	}
}

// 149 plain resource attrs + 2 cloudwatch.* scope stamps = 151; excess 1.
// Force-prune must drop a plain resource attr, not the cloudwatch.* scope stamp
// (which previously lost alphabetically: cloudwatch.source > cloudwatch.solution).
func TestForcePrune_KeepsCloudwatchScope(t *testing.T) {
	resource := map[string]string{}
	for i := range 149 {
		resource[fmt.Sprintf("parity.attr.%03d", i)] = "v"
	}
	scope := map[string]string{
		"cloudwatch.source":   "otel",
		"cloudwatch.solution": "profiles",
	}
	md := newTestMetrics(resource, scope, nil)
	p := newProcessor(&Config{MaxTotalAttributes: 150}, zap.NewNop())
	_, err := p.processMetrics(t.Context(), md)
	require.NoError(t, err)

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Scope().Attributes()
	for _, k := range []string{"cloudwatch.source", "cloudwatch.solution"} {
		_, ok := sm.Get(k)
		assert.Truef(t, ok, "cloudwatch scope stamp %q must survive force-prune", k)
	}
	assert.Equal(t, 148, getResourceAttrs(md).Len(), "one plain resource attr dropped instead")
}

// Last-resort corner: the record carries more identity attributes than the limit
// (a misconfiguration). Force-prune pass 2 drops identity too -- alphabetically
// from the end -- to still guarantee the hard limit, and logs an error.
func TestForcePrune_LastResortDropsIdentity(t *testing.T) {
	resource := map[string]string{
		"service.name":        "svc",
		"service.instance.id": "i-1",
		"cloud.resource_id":   "arn:aws:...",
	}
	md := newTestMetrics(resource, nil, nil)

	core, logs := observer.New(zapcore.DebugLevel)
	p := newProcessor(&Config{MaxTotalAttributes: 2}, zap.New(core))
	_, err := p.processMetrics(t.Context(), md)
	require.NoError(t, err)

	attrs := getResourceAttrs(md)
	assert.Equal(t, 2, attrs.Len(), "hard limit still guaranteed in the last resort")
	// service.name sorts last among the three, so it is the one force-pruned.
	_, ok := attrs.Get("service.name")
	assert.False(t, ok, "last resort drops the alphabetically-last identity key")
	assert.GreaterOrEqual(t, len(logs.FilterLevelExact(zapcore.ErrorLevel).All()), 1, "force-prune exhaustion logs an error")
}
