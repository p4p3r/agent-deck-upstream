package scripts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const officialAgentDeckRepository = "https://github.com/asheshgoplani/agent-deck"

type officialProvenance struct {
	Repository string `json:"repository"`
	Ref        string `json:"ref"`
	Commit     string `json:"commit"`
	Tree       string `json:"tree"`
	Archive    struct {
		Format string `json:"format"`
		Prefix string `json:"prefix"`
		SHA256 string `json:"sha256"`
	} `json:"archive"`
	ExternalReleaseVerification struct {
		Status              string `json:"status"`
		OfflineFixtureScope string `json:"offline_fixture_scope"`
	} `json:"external_release_verification"`
}

func commandOutput(t *testing.T, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func verifyReproducibleBuildPlan(plan, wantCommit string) error {
	required := []string{
		"git status --porcelain=v1 -uall",
		"CGO_ENABLED=0",
		"GOTOOLCHAIN=go1.25.13",
		"-mod=readonly",
		"-trimpath",
		"-buildvcs=false",
		"-buildid=",
		"main.SourceCommit=" + wantCommit,
	}
	for _, needle := range required {
		if !strings.Contains(plan, needle) {
			return fmt.Errorf("missing %q", needle)
		}
	}
	return nil
}

func loadOfficialProvenance(t *testing.T) officialProvenance {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "phase1-official-provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var provenance officialProvenance
	if err := json.Unmarshal(data, &provenance); err != nil {
		t.Fatal(err)
	}
	return provenance
}

func validateOfficialProvenanceRecord(t *testing.T, p officialProvenance) {
	t.Helper()
	if p.Repository != officialAgentDeckRepository || p.Ref != "refs/tags/v1.16.8" {
		t.Fatalf("unexpected source identity: repository=%q ref=%q", p.Repository, p.Ref)
	}
	if len(p.Commit) != 40 || len(p.Tree) != 40 || len(p.Archive.SHA256) != 64 {
		t.Fatal("pinned commit/tree/archive digests must be full length")
	}
	for name, digest := range map[string]string{
		"commit":          p.Commit,
		"tree":            p.Tree,
		"archive SHA-256": p.Archive.SHA256,
	} {
		if _, err := hex.DecodeString(digest); err != nil {
			t.Fatalf("%s is not hexadecimal: %v", name, err)
		}
	}
	if p.Archive.Format != "tar" || p.Archive.Prefix != "agent-deck-v1.16.8/" {
		t.Fatalf("invalid archive recipe: %+v", p.Archive)
	}
}

func TestPhase1PinnedSourceProvenanceRecordIsValidOffline(t *testing.T) {
	validateOfficialProvenanceRecord(t, loadOfficialProvenance(t))
}

func TestPhase1PinnedSourceProvenanceIsOfflineReproducible(t *testing.T) {
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	p := loadOfficialProvenance(t)
	validateOfficialProvenanceRecord(t, p)
	available := exec.Command("git", "cat-file", "-e", p.Commit+"^{commit}")
	available.Dir = repoRoot
	if err := available.Run(); err != nil {
		t.Skipf("record validated; pinned commit %s is unavailable in this checkout, so archive replay requires the documented explicit fetch/verification", p.Commit)
	}
	if got := strings.TrimSpace(commandOutput(t, repoRoot, "git", "cat-file", "-t", p.Commit)); got != "commit" {
		t.Fatalf("pinned object type = %q, want commit", got)
	}
	if got := strings.TrimSpace(commandOutput(t, repoRoot, "git", "show", "-s", "--format=%T", p.Commit)); got != p.Tree {
		t.Fatalf("pinned tree = %q, record says %q", got, p.Tree)
	}
	cmd := exec.Command("git", "archive", "--format="+p.Archive.Format, "--prefix="+p.Archive.Prefix, p.Commit)
	cmd.Dir = repoRoot
	archive, err := cmd.Output()
	if err != nil {
		t.Fatalf("reproduce pinned git archive: %v", err)
	}
	sum := sha256.Sum256(archive)
	if got := hex.EncodeToString(sum[:]); got != p.Archive.SHA256 {
		t.Fatalf("archive SHA-256 = %s, record says %s", got, p.Archive.SHA256)
	}

	mutated := strings.Repeat("0", 40)
	check := exec.Command("git", "cat-file", "-e", mutated+"^{commit}")
	check.Dir = repoRoot
	if out, err := check.CombinedOutput(); err == nil {
		t.Fatalf("mutated commit unexpectedly resolved: %s", out)
	}
}

func TestExternalReleaseEvidenceIsNotClaimedAsOfflineVerification(t *testing.T) {
	p := loadOfficialProvenance(t)
	if !strings.Contains(p.ExternalReleaseVerification.Status, "independently_verified") {
		t.Fatalf("external evidence is not labeled by origin: %q", p.ExternalReleaseVerification.Status)
	}
	if !strings.Contains(p.ExternalReleaseVerification.OfflineFixtureScope, "do not repeat") {
		t.Fatalf("offline proof boundary is unclear: %q", p.ExternalReleaseVerification.OfflineFixtureScope)
	}
}

func TestReproducibleBuildPlanVerifierRejectsMutatedRevision(t *testing.T) {
	want := "9c884abdba47def96b2b811c87978def95ef79cf"
	valid := strings.Join([]string{
		"git status --porcelain=v1 -uall",
		"CGO_ENABLED=0",
		"GOTOOLCHAIN=go1.25.13",
		"go build -mod=readonly -trimpath -buildvcs=false",
		"-ldflags -s -w -buildid= -X main.SourceCommit=" + want,
	}, "\n")
	if err := verifyReproducibleBuildPlan(valid, want); err != nil {
		t.Fatalf("valid build-plan control rejected: %v", err)
	}
	mutated := strings.Replace(valid, want, strings.Repeat("0", 40), 1)
	if err := verifyReproducibleBuildPlan(mutated, want); err == nil {
		t.Fatal("revision-mutated negative control was accepted")
	}
}

func TestPhase1ReproducibleBuildContractIdentifiesExactSourceCommit(t *testing.T) {
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	head := strings.TrimSpace(commandOutput(t, repoRoot, "git", "rev-parse", "HEAD"))
	plan := commandOutput(t, repoRoot, "make", "-n", "build-reproducible")
	if err := verifyReproducibleBuildPlan(plan, head); err != nil {
		t.Fatalf("reproducible build plan does not identify its exact source commit: %v\n%s", err, plan)
	}
	shortHead := strings.TrimSpace(commandOutput(t, repoRoot, "git", "rev-parse", "--short=12", "HEAD"))
	wantVersion := "1.16.8+p4p3r." + shortHead
	if !strings.Contains(plan, "main.Version="+wantVersion) {
		t.Fatalf("reproducible build version does not use SemVer build metadata %q:\n%s", wantVersion, plan)
	}
	gotmpAssignments := regexp.MustCompile(`GOTMPDIR="([^"]*)"`).FindAllStringSubmatch(plan, -1)
	if len(gotmpAssignments) == 0 {
		t.Fatalf("reproducible build plan has no GOTMPDIR assignment:\n%s", plan)
	}
	for _, match := range gotmpAssignments {
		if match[1] == "" || !filepath.IsAbs(match[1]) {
			t.Fatalf("reproducible build plan uses non-isolated GOTMPDIR %q:\n%s", match[1], plan)
		}
	}

	makefile, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	makeSource := string(makefile)
	for _, required := range []string{
		`REPRO_VERSION ?= $(REPRO_BASE_VERSION)+p4p3r.$(REPRO_SHORT_COMMIT)`,
		`REPRO_GOTMPDIR ?= $(abspath $(BUILD_DIR)/.gotmp)`,
		`source_dir="$$scratch/source-$$slot"`,
		`REPRO_GOCACHE="$$scratch/gocache-$$slot"`,
		`REPRO_GOTMPDIR="$$scratch/gotmp-$$slot"`,
		`git archive --format=tar "$(REPRO_SOURCE_COMMIT)"`,
		`cmp "$$scratch/output-first/$(BINARY_NAME)" "$$scratch/output-second/$(BINARY_NAME)"`,
	} {
		if !strings.Contains(makeSource, required) {
			t.Fatalf("verify-reproducible-build is missing independent-build guard %q", required)
		}
	}

	doc, err := os.ReadFile(filepath.Join(repoRoot, "docs", "SOURCE-PROVENANCE.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), "Plain `go build` is not a provenance build") {
		t.Fatal("plain-build provenance hazard is not documented")
	}
}

func TestPlainGoBuildMetadataIsInsufficientForProvenance(t *testing.T) {
	head := strings.Repeat("a", 40)
	plainMetadata := "path github.com/asheshgoplani/agent-deck/cmd/agent-deck\n" +
		"build\tvcs.revision=" + head + "\nbuild\tvcs.modified=true\n"
	if err := verifyReproducibleBuildPlan(plainMetadata, head); err == nil {
		t.Fatalf("plain go build metadata was accepted as the supported provenance contract:\n%s", plainMetadata)
	}
}
