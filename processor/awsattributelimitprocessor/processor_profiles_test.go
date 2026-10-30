// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsattributelimitprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.uber.org/zap/zapcore"
)

// --- profiles test helpers ---

// newTestProfiles builds a single ResourceProfiles/ScopeProfiles/Profile and
// returns handles to set attributes on each level.
func newTestProfiles() (pd pprofile.Profiles, resource, scope pcommon.Map, dic pprofile.ProfilesDictionary, prof pprofile.Profile) {
	pd = pprofile.NewProfiles()
	rp := pd.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	prof = sp.Profiles().AppendEmpty()
	return pd, rp.Resource().Attributes(), sp.Scope().Attributes(), pd.Dictionary(), prof
}

// addProfileAttr adds a profile-level attribute via the dictionary: it interns
// the key in the string table, appends a KeyValueAndUnit to the shared attribute
// table, and references it from the profile's attribute indices.
func addProfileAttr(dic pprofile.ProfilesDictionary, prof pprofile.Profile, key string) {
	st := dic.StringTable()
	keyIdx := -1
	for i := 0; i < st.Len(); i++ {
		if st.At(i) == key {
			keyIdx = i
			break
		}
	}
	if keyIdx == -1 {
		st.Append(key)
		keyIdx = st.Len() - 1
	}
	kv := dic.AttributeTable().AppendEmpty()
	kv.SetKeyStrindex(int32(keyIdx))
	kv.Value().SetStr("v")
	prof.AttributeIndices().Append(int32(dic.AttributeTable().Len() - 1))
}

// profileAttrKeys returns the resolved keys currently referenced by the profile.
func profileAttrKeys(dic pprofile.ProfilesDictionary, prof pprofile.Profile) map[string]struct{} {
	leaf := profileAttrLeaf{indices: prof.AttributeIndices(), table: dic.AttributeTable(), strings: dic.StringTable()}
	keys := make(map[string]struct{})
	leaf.rangeKeys(func(k string) bool {
		keys[k] = struct{}{}
		return true
	})
	return keys
}

func totalProfileAttrs(resource, scope pcommon.Map, prof pprofile.Profile) int {
	return resource.Len() + scope.Len() + prof.AttributeIndices().Len()
}

// --- tests ---

func TestProfiles_UnderLimit_NoChange(t *testing.T) {
	pd, resource, scope, dic, prof := newTestProfiles()
	resource.PutStr("service.name", "svc")
	scope.PutStr("scope.attr", "v")
	for i := range 10 {
		addProfileAttr(dic, prof, "leaf."+string(rune('a'+i)))
	}
	before := totalProfileAttrs(resource, scope, prof)
	require.Equal(t, 12, before)

	p, logs := newTestProcessorWithLogs(150)
	_, err := p.processProfiles(t.Context(), pd)
	require.NoError(t, err)

	assert.Equal(t, before, totalProfileAttrs(resource, scope, prof), "nothing should be dropped under the limit")
	assert.Empty(t, logs.FilterLevelExact(zapcore.WarnLevel).All())
}

func TestProfiles_TrimsToLimit(t *testing.T) {
	pd, resource, scope, dic, prof := newTestProfiles()
	// 5 non-droppable identity resource attrs.
	resource.PutStr("service.name", "svc")
	resource.PutStr("service.instance.id", "i-1")
	resource.PutStr("k8s.pod.uid", "uid-1")
	resource.PutStr("cloud.resource_id", "arn:...")
	resource.PutStr("telemetry.sdk.name", "otel")
	// 160 droppable (tier 10) profile attrs -> total 165, limit 150, excess 15.
	for i := range 160 {
		addProfileAttr(dic, prof, "leaf.key"+pad(i))
	}
	tableLenBefore := dic.AttributeTable().Len()

	p, _ := newTestProcessorWithLogs(150)
	_, err := p.processProfiles(t.Context(), pd)
	require.NoError(t, err)

	assert.Equal(t, 150, totalProfileAttrs(resource, scope, prof), "total must be trimmed to exactly the limit")
	assert.Equal(t, 145, prof.AttributeIndices().Len(), "15 profile attrs dropped (the leaf/last-resort tier)")
	assert.Equal(t, 5, resource.Len(), "identity/non-droppable resource attrs must be kept")
	assert.Equal(t, tableLenBefore, dic.AttributeTable().Len(), "shared dictionary attribute table must not be mutated")
}

func TestProfiles_NeverDropsIdentity(t *testing.T) {
	pd, resource, scope, dic, prof := newTestProfiles()
	resource.PutStr("service.name", "svc")
	resource.PutStr("service.instance.id", "i-1")
	resource.PutStr("k8s.pod.uid", "uid-1")
	resource.PutStr("k8s.pod.name", "pod-1")
	resource.PutStr("cloud.resource_id", "arn:...")
	// 20 droppable profile attrs. Limit 5 forces all profile attrs to go.
	for i := range 20 {
		addProfileAttr(dic, prof, "leaf.key"+pad(i))
	}

	p, _ := newTestProcessorWithLogs(5)
	_, err := p.processProfiles(t.Context(), pd)
	require.NoError(t, err)

	assert.Equal(t, 5, totalProfileAttrs(resource, scope, prof))
	assert.Equal(t, 0, prof.AttributeIndices().Len(), "profile leaf attrs are dropped first to protect identity")
	for _, k := range []string{"service.name", "service.instance.id", "k8s.pod.uid", "k8s.pod.name", "cloud.resource_id"} {
		_, ok := resource.Get(k)
		assert.Truef(t, ok, "identity attr %q must never be dropped", k)
	}
}

func TestProfiles_TierOrder_ResourceLabelsBeforeProfileAttrs(t *testing.T) {
	// Mirrors the metrics policy: droppable resource k8s labels (tiers 1-8) are
	// shed before profile/leaf attrs (tier 10). With excess 1, the droppable
	// resource label must go and all profile attrs must survive.
	pd, resource, scope, dic, prof := newTestProfiles()
	resource.PutStr("service.name", "svc")               // protected
	resource.PutStr("k8s.node.label.custom-node", "val") // droppable tier 6
	addProfileAttr(dic, prof, "leaf.a")
	addProfileAttr(dic, prof, "leaf.b")
	// total = 2 resource + 0 scope + 2 profile = 4; limit 3; excess 1.

	p, _ := newTestProcessorWithLogs(3)
	_, err := p.processProfiles(t.Context(), pd)
	require.NoError(t, err)

	assert.Equal(t, 3, totalProfileAttrs(resource, scope, prof))
	_, labelKept := resource.Get("k8s.node.label.custom-node")
	assert.False(t, labelKept, "droppable resource label should be dropped before any profile attr")
	keys := profileAttrKeys(dic, prof)
	assert.Contains(t, keys, "leaf.a")
	assert.Contains(t, keys, "leaf.b")
}

func TestProfiles_ForcePruneWhenAllProtected(t *testing.T) {
	// All remaining attrs are protected; force-prune guarantees the hard limit
	// is still met and logs an error.
	pd, resource, scope, dic, prof := newTestProfiles()
	resource.PutStr("service.name", "svc")
	resource.PutStr("k8s.pod.uid", "uid-1")
	resource.PutStr("cloud.resource_id", "arn:...")
	_ = dic

	p, logs := newTestProcessorWithLogs(2)
	_, err := p.processProfiles(t.Context(), pd)
	require.NoError(t, err)

	assert.Equal(t, 2, totalProfileAttrs(resource, scope, prof), "force-prune must guarantee the hard limit")
	assert.GreaterOrEqual(t, len(logs.FilterLevelExact(zapcore.ErrorLevel).All()), 1, "force-prune should log an error")
}

func TestProfiles_SharedResourceTrimmedOncePerResourceProfiles(t *testing.T) {
	// Two profiles under one ResourceProfiles share the resource attrs; a drop
	// triggered by the first is visible to the second (by design, as in metrics).
	pd := pprofile.NewProfiles()
	rp := pd.ResourceProfiles().AppendEmpty()
	resource := rp.Resource().Attributes()
	resource.PutStr("service.name", "svc")
	resource.PutStr("k8s.node.label.droppable", "v") // tier 6
	dic := pd.Dictionary()
	sp := rp.ScopeProfiles().AppendEmpty()
	p1 := sp.Profiles().AppendEmpty()
	p2 := sp.Profiles().AppendEmpty()
	addProfileAttr(dic, p1, "leaf.a")
	addProfileAttr(dic, p2, "leaf.b")
	// Each profile: 2 resource + 1 leaf = 3; limit 2; excess 1 -> drop the label.

	p, _ := newTestProcessorWithLogs(2)
	_, err := p.processProfiles(t.Context(), pd)
	require.NoError(t, err)

	_, labelKept := resource.Get("k8s.node.label.droppable")
	assert.False(t, labelKept, "shared droppable resource label trimmed")
	assert.Equal(t, 1, p1.AttributeIndices().Len(), "leaf.a kept")
	assert.Equal(t, 1, p2.AttributeIndices().Len(), "leaf.b kept")
}

// pad renders i as a fixed-width zero-padded string so generated keys are unique
// and sort stably.
func pad(i int) string {
	const digits = "0123456789"
	return string([]byte{digits[(i/100)%10], digits[(i/10)%10], digits[i%10]})
}
