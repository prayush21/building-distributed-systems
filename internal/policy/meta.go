package policy

// Metadata size estimates.
//
// MetadataBytes exists to answer "what does this policy's bookkeeping cost per
// cached object", so that a policy which buys a lower miss ratio with a large
// per-object index is not scored as a free win. The numbers below are 64-bit Go
// estimates, not measured heap: they are consistent across policies, which is
// what makes the comparison meaningful.
const (
	// StringHeaderBytes is a Go string header (pointer + length).
	StringHeaderBytes = 16

	// PointerBytes is one pointer.
	PointerBytes = 8

	// MapEntryBytes approximates the per-entry cost of a Go map beyond the
	// key and value themselves: bucket slots, tophash, and the load-factor
	// slack that keeps a map under ~6.5/8 occupancy.
	MapEntryBytes = 24

	// SliceElemBytes is the per-element cost of a string in a slice, before
	// the key bytes themselves.
	SliceElemBytes = StringHeaderBytes
)

// TrackedKeyBytes is the estimated cost of tracking one key in a map whose
// value is valueBytes wide. Callers keep a running sum so MetadataBytes stays
// O(1).
func TrackedKeyBytes(key string, valueBytes int) int {
	return StringHeaderBytes + len(key) + valueBytes + MapEntryBytes
}
