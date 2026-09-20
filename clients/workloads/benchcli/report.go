package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/runrecord"
)

// runReport renders a run.json into results.tsv (the flat per-cell view) and
// results.md (the human report: median ± MAD tables per mode within each tier,
// a scorecard, and the clock/corpus header). It asserts nothing — "report,
// don't assert" — the correctness gate lives in `benchcli correctness`
// (design §8.4, §8.5, §9.3).
func runReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	runPath := fs.String("run", "", "path to run.json (required)")
	outDir := fs.String("out-dir", "", "directory for results.tsv/results.md (default: the run.json's directory)")
	stdout := fs.Bool("stdout", false, "also print results.md to stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runPath == "" {
		return fmt.Errorf("-run is required (path to run.json)")
	}
	run, err := runrecord.Load(*runPath)
	if err != nil {
		return err
	}
	dir := *outDir
	if dir == "" {
		dir = filepath.Dir(*runPath)
	}

	tsv := renderTSV(run)
	md := renderMarkdown(run)

	tsvPath := filepath.Join(dir, "results.tsv")
	mdPath := filepath.Join(dir, "results.md")
	if err := os.WriteFile(tsvPath, []byte(tsv), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		return err
	}
	if *stdout {
		fmt.Print(md)
	}
	fmt.Fprintf(os.Stderr, "wrote %s and %s (%d cells, %d aggregated)\n",
		tsvPath, mdPath, len(run.Cells), len(runrecord.Aggregate(run.Cells)))
	return nil
}

// renderTSV is the flat per-(cell,repeat) dump: identity columns, then every
// NumericColumns value, then the booleans and hgrm path. One row per repeat so
// nothing is hidden behind the median.
func renderTSV(run *runrecord.Run) string {
	cols := runrecord.NumericColumns()
	var b strings.Builder
	head := append([]string{
		"id", "repeat", "tier", "transport", "codec", "fixture", "pool", "gc", "mode", "inflight", "rate",
	}, colNames(cols)...)
	head = append(head, "one_way_published", "broker_throttled", "hgrm", "grafana")
	b.WriteString(strings.Join(head, "\t") + "\n")
	for _, c := range run.Cells {
		s := c.Summary
		row := []string{
			c.ID, strconv.Itoa(c.Repeat), s.Tier, s.Transport, s.Codec, s.Fixture, s.Pool, s.GC, s.Mode,
			strconv.Itoa(s.Inflight), trimFloat(s.Rate, 0),
		}
		for _, col := range cols {
			row = append(row, trimFloat(col.Get(s), col.Dec))
		}
		row = append(row, strconv.FormatBool(s.OneWayPublished), strconv.FormatBool(s.BrokerThrottled), c.HGRM, s.Grafana)
		b.WriteString(strings.Join(row, "\t") + "\n")
	}
	return b.String()
}

func colNames(cols []runrecord.Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

// mdColumns is the curated, mode-appropriate Markdown column set: header text,
// the NumericColumns key, and whether to show ± MAD next to the median.
type mdColumn struct {
	head string
	key  string
	mad  bool
}

// columnsForMode returns the results.md columns for a run mode. Closed-loop
// modes lead with RTT percentiles; open-loop modes lead with throughput and
// late sends; fault leads with the integrity counters (design §8.2, §8.4).
func columnsForMode(mode string) []mdColumn {
	switch mode {
	case "openloop", "saturation":
		return []mdColumn{
			{"tput msg/s", "throughput_msg_s", false},
			{"p50 µs", "rtt_p50_us", true},
			{"p99 µs", "rtt_p99_us", true},
			{"p99.9 µs", "rtt_p999_us", false},
			{"co-p99 µs", "rtt_co_corrected_p99_us", false},
			{"late", "late_sends", false},
			{"errors", "errors", false},
			{"alloc/msg", "driver_allocs_per_msg", false},
		}
	case "fault":
		return []mdColumn{
			{"msgs", "msgs", false},
			{"missing", "missing", false},
			{"dup", "duplicate", false},
			{"reord", "reordered", false},
			{"redeliv", "redelivered", false},
			{"p99 µs", "rtt_p99_us", false},
			{"errors", "errors", false},
		}
	default: // latency, windowed, coldstart
		return []mdColumn{
			{"p50 µs", "rtt_p50_us", true},
			{"p90 µs", "rtt_p90_us", false},
			{"p99 µs", "rtt_p99_us", true},
			{"p99.9 µs", "rtt_p999_us", false},
			{"max µs", "rtt_max_us", false},
			{"tput msg/s", "throughput_msg_s", false},
			{"srv p99 µs", "server_duration_p99_us", false},
			{"alloc/msg", "driver_allocs_per_msg", false},
			{"gc pause p99 µs", "driver_gc_pause_p99_us", false},
			{"errors", "errors", false},
		}
	}
}

// tierOrder is the delivery-semantics ordering for the report (design §2.3).
var tierOrder = []string{"rpc", "at_least_once", "at_most_once"}

func renderMarkdown(run *runrecord.Run) string {
	agg := runrecord.Aggregate(run.Cells)
	var b strings.Builder

	fmt.Fprintf(&b, "# proto-bench results — `%s`\n\n", run.RunID)
	dirty := ""
	if run.Git.Dirty {
		dirty = " (dirty)"
	}
	fmt.Fprintf(&b, "- git: `%s`%s\n", run.Git.Rev, dirty)
	if run.StartedAt != "" {
		fmt.Fprintf(&b, "- started: %s → finished: %s\n", run.StartedAt, run.FinishedAt)
	}
	if len(run.Versions) > 0 {
		fmt.Fprintf(&b, "- versions: %s\n", kvLine(run.Versions))
	}
	if run.Host.CPU != "" || run.Host.DriverCPUSet != "" {
		fmt.Fprintf(&b, "- host: %s (driver cpuset %s)\n", run.Host.CPU, run.Host.DriverCPUSet)
	}
	b.WriteString("\n")

	writeClockSection(&b, run)
	writeCorpusSection(&b, run)

	// Group aggregated cells by tier, then mode, preserving first-seen order.
	byTier := map[string]map[string][]runrecord.Aggregated{}
	tierSeen := []string{}
	for _, a := range agg {
		tier := orUnknown(a.Ident.Tier)
		mode := orUnknown(a.Ident.Mode)
		if _, ok := byTier[tier]; !ok {
			byTier[tier] = map[string][]runrecord.Aggregated{}
			tierSeen = append(tierSeen, tier)
		}
		byTier[tier][mode] = append(byTier[tier][mode], a)
	}

	for _, tier := range orderedKeys(tierSeen, tierOrder) {
		fmt.Fprintf(&b, "## Tier: %s\n\n", tier)
		modes := byTier[tier]
		for _, mode := range sortedKeys(modes) {
			fmt.Fprintf(&b, "### Mode: %s\n\n", mode)
			writeCellTable(&b, mode, modes[mode])
			b.WriteString("\n")
		}
	}

	writeScorecard(&b, agg)
	return b.String()
}

// writeCellTable renders one median ± MAD table for a (tier, mode) group.
func writeCellTable(b *strings.Builder, mode string, cells []runrecord.Aggregated) {
	cols := columnsForMode(mode)
	head := []string{"transport", "codec", "fixture", "pool", "gc", knob(mode)}
	for _, c := range cols {
		head = append(head, c.head)
	}
	head = append(head, "stable")
	writeRow(b, head)
	writeRow(b, sep(len(head)))
	for _, a := range sortAgg(cells) {
		row := []string{a.Ident.Transport, a.Ident.Codec, a.Ident.Fixture, a.Ident.Pool, a.Ident.GC, knobValue(mode, a.Ident)}
		for _, c := range cols {
			row = append(row, cellValue(a, c))
		}
		if a.Unstable {
			row = append(row, fmt.Sprintf("⚠ unstable (MAD %s)", pctOf(a.MAD[a.StableBy], a.Med[a.StableBy])))
		} else {
			row = append(row, "ok")
		}
		writeRow(b, row)
	}
}

// cellValue formats one median cell, appending ± MAD when the column asks for it
// and the MAD is non-zero.
func cellValue(a runrecord.Aggregated, c mdColumn) string {
	med, ok := a.Med[c.key]
	if !ok {
		return "-"
	}
	dec := decFor(c.key)
	s := trimFloat(med, dec)
	if c.mad {
		if d := a.MAD[c.key]; d > 0 {
			s += " ±" + trimFloat(d, dec)
		}
	}
	return s
}

func writeClockSection(b *strings.Builder, run *runrecord.Run) {
	if len(run.Clock) == 0 {
		return
	}
	b.WriteString("## Clock\n\n")
	writeRow(b, []string{"region", "offset µs", "uncertainty µs", "source", "synced"})
	writeRow(b, sep(5))
	for _, region := range sortedStringKeys(run.Clock) {
		c := run.Clock[region]
		writeRow(b, []string{
			region,
			trimFloat(float64(c.OffsetNs)/1000, 2),
			trimFloat(float64(c.UncertaintyNs)/1000, 2),
			orUnknown(c.Source),
			strconv.FormatBool(c.Synced),
		})
	}
	b.WriteString("\n")
}

func writeCorpusSection(b *strings.Builder, run *runrecord.Run) {
	if len(run.Corpus.Fixtures) == 0 {
		return
	}
	fmt.Fprintf(b, "## Corpus (seed %d)\n\n", run.Corpus.Seed)
	writeRow(b, []string{"fixture", "proto B", "protojson B", "vtproto B"})
	writeRow(b, sep(4))
	for _, f := range sortedStringKeys(run.Corpus.Fixtures) {
		fx := run.Corpus.Fixtures[f]
		writeRow(b, []string{f, itoa(fx.ProtoBytes), itoa(fx.ProtoJSONBytes), itoa(fx.VTProtoBytes)})
	}
	b.WriteString("\n")
}

// writeScorecard picks a few headline comparisons out of the aggregated cells:
// the lowest-latency cell per tier, and the pool=all vs pool=none allocation
// win on matching cells (design §8.5 "scorecard", §15 acceptance).
func writeScorecard(b *strings.Builder, agg []runrecord.Aggregated) {
	b.WriteString("## Scorecard\n\n")

	// Lowest rtt_p50 per tier among latency cells.
	best := map[string]runrecord.Aggregated{}
	for _, a := range agg {
		if a.Ident.Mode != "latency" || a.Med["rtt_p50_us"] <= 0 {
			continue
		}
		tier := orUnknown(a.Ident.Tier)
		if cur, ok := best[tier]; !ok || a.Med["rtt_p50_us"] < cur.Med["rtt_p50_us"] {
			best[tier] = a
		}
	}
	if len(best) > 0 {
		b.WriteString("Lowest-latency cell per tier (latency mode):\n\n")
		for _, tier := range orderedKeys(mapKeys(best), tierOrder) {
			a := best[tier]
			fmt.Fprintf(b, "- **%s**: %s/%s/%s p50 %s µs\n",
				tier, a.Ident.Transport, a.Ident.Codec, a.Ident.Fixture, trimFloat(a.Med["rtt_p50_us"], 0))
		}
		b.WriteString("\n")
	}

	// pool=all vs pool=none allocation win, matched on everything but pool.
	type key struct{ transport, codec, fixture, gc, mode string }
	byKey := map[key]map[string]runrecord.Aggregated{}
	for _, a := range agg {
		k := key{a.Ident.Transport, a.Ident.Codec, a.Ident.Fixture, a.Ident.GC, a.Ident.Mode}
		if byKey[k] == nil {
			byKey[k] = map[string]runrecord.Aggregated{}
		}
		byKey[k][a.Ident.Pool] = a
	}
	wrote := false
	for _, k := range sortKeys(byKey) {
		pools := byKey[k]
		all, okAll := pools["all"]
		none, okNone := pools["none"]
		if !okAll || !okNone {
			continue
		}
		na := none.Med["driver_allocs_per_msg"]
		aa := all.Med["driver_allocs_per_msg"]
		if na <= 0 && aa <= 0 {
			continue
		}
		if !wrote {
			b.WriteString("Driver allocations per message, pool=none → pool=all:\n\n")
			wrote = true
		}
		fmt.Fprintf(b, "- %s/%s/%s: %s → %s allocs/msg\n",
			k.transport, k.codec, k.fixture, trimFloat(na, 2), trimFloat(aa, 2))
	}
	if !wrote && len(best) == 0 {
		b.WriteString("_(no comparable cells in this run)_\n")
	}
}

// --- small formatting helpers ---

func knob(mode string) string {
	switch mode {
	case "openloop", "saturation", "fault":
		return "rate"
	default:
		return "inflight"
	}
}

func knobValue(mode string, s runrecord.Summary) string {
	if knob(mode) == "rate" {
		return trimFloat(s.Rate, 0)
	}
	if s.Inflight == 0 {
		return "1"
	}
	return itoa(s.Inflight)
}

// decFor returns the display precision for a column key from NumericColumns.
func decFor(key string) int {
	for _, c := range runrecord.NumericColumns() {
		if c.Name == key {
			return c.Dec
		}
	}
	return 0
}

func writeRow(b *strings.Builder, cells []string) {
	b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
}

func sep(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "---"
	}
	return out
}

// trimFloat formats f with dec decimals, trimming a trailing ".0…" so integers
// stay clean in the TSV and tables.
func trimFloat(f float64, dec int) string {
	s := strconv.FormatFloat(f, 'f', dec, 64)
	if dec > 0 && strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}

func itoa(i int) string { return strconv.Itoa(i) }

func pctOf(part, whole float64) string {
	if whole == 0 {
		return "n/a"
	}
	return trimFloat(part/whole*100, 0) + "%"
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func kvLine(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + " " + m[k]
	}
	return strings.Join(parts, ", ")
}

// sortAgg orders cells within a table by transport, codec, fixture, pool, gc,
// then the concurrency knob, so runs render deterministically.
func sortAgg(cells []runrecord.Aggregated) []runrecord.Aggregated {
	out := append([]runrecord.Aggregated(nil), cells...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i].Ident, out[j].Ident
		for _, pair := range [][2]string{
			{a.Transport, b.Transport}, {a.Codec, b.Codec}, {a.Fixture, b.Fixture},
			{a.Pool, b.Pool}, {a.GC, b.GC},
		} {
			if pair[0] != pair[1] {
				return pair[0] < pair[1]
			}
		}
		if a.Inflight != b.Inflight {
			return a.Inflight < b.Inflight
		}
		return a.Rate < b.Rate
	})
	return out
}

// orderedKeys returns keys ordered by pref first (for keys present), then any
// remaining keys sorted.
func orderedKeys(keys, pref []string) []string {
	have := map[string]bool{}
	for _, k := range keys {
		have[k] = true
	}
	var out []string
	used := map[string]bool{}
	for _, p := range pref {
		if have[p] {
			out = append(out, p)
			used[p] = true
		}
	}
	rest := []string{}
	for _, k := range keys {
		if !used[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStringKeys[V any](m map[string]V) []string { return sortedKeys(m) }

func mapKeys[V any](m map[string]V) []string { return sortedKeys(m) }

func sortKeys[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprintf("%v", out[i]) < fmt.Sprintf("%v", out[j])
	})
	return out
}
