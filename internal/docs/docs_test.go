// Package docs holds no code. It exists so that the documentation standard in
// docs/conventions.md is a test rather than a habit.
//
// A standard nobody checks decays at the first deadline. This walks the module
// and asserts the mechanical parts of the standard, so a package added in a
// later phase that skips its chapter fails the suite the day it is written
// rather than the day someone notices.
//
// It cannot check whether an explanation is good — no test can. The six
// headings are chosen so that filling them in dishonestly is more work than
// filling them in honestly.
package docs_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot is two levels up: internal/docs -> internal -> the module root.
const repoRoot = "../.."

// sectionSign is the character D4 forbids outside a chapter, written as an
// escape rather than as itself.
//
// Spelling it literally here would make this file fail its own check, and
// exempting the file would be worse: a rule its own enforcement cannot satisfy
// is a rule with a hole in it.
const sectionSign = "\u00a7"

// requiredHeadings are the six chapter headings from docs/conventions.md, in the
// order a chapter must present them.
var requiredHeadings = []string{
	"# The problem",
	"# Why the obvious fixes do not work",
	"# What this package does",
	"# What it deliberately does not do",
	"# Reading order",
	"# Where this comes from",
}

// exemptFromChapter lists packages that need a package comment (D1) but not a
// six-heading chapter (D2, D3), with the reason for each.
//
// A named list rather than a path pattern, so a reader can see what is exempt
// and why without reverse-engineering a glob, and so adding to it is a visible
// decision rather than a quiet one.
var exemptFromChapter = map[string]string{
	"internal/inventory/migrations": "a dozen lines embedding a directory of SQL; six headings would be padding",
	"internal/booking/migrations":   "a dozen lines embedding a directory of SQL; six headings would be padding",
}

// exemptEntirely lists packages with no non-test .go file at all.
//
// Go forbids a non-_test.go file from declaring an external test package, so a
// doc.go for these cannot be written: their package clause is `foo_test`. Their
// package comment lives in the test file that carries it.
var exemptEntirely = map[string]string{
	"internal/integration": "test-only package, declares package integration_test",
	"internal/toolchain":   "test-only package, guards the Go version floor",
	"internal/docs":        "test-only package; this is the enforcement itself",
}

// pkg is one Go package found on disk.
type pkg struct {
	path      string   // repo-relative, e.g. internal/platform/outbox
	dir       string   // absolute
	goFiles   []string // non-test .go files, base names
	testFiles []string // _test.go files, base names
}

// findPackages walks the module for directories holding Go files.
//
// It walks rather than using `go list` so that a package which does not compile
// is still checked: a chapter is missing whether or not the code builds, and a
// checker that goes quiet exactly when things are broken is worse than none.
func findPackages(t *testing.T) []pkg {
	t.Helper()
	var found []pkg
	for _, root := range []string{"internal", "cmd", "contracts"} {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			switch d.Name() {
			case "testdata", ".git", ".superpowers":
				return filepath.SkipDir
			}
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			p := pkg{dir: path}
			for _, e := range entries {
				name := e.Name()
				if e.IsDir() || !strings.HasSuffix(name, ".go") {
					continue
				}
				if strings.HasSuffix(name, "_test.go") {
					p.testFiles = append(p.testFiles, name)
				} else {
					p.goFiles = append(p.goFiles, name)
				}
			}
			if len(p.goFiles) == 0 && len(p.testFiles) == 0 {
				return nil
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			p.path = filepath.ToSlash(rel)
			found = append(found, p)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
	}
	if len(found) == 0 {
		t.Fatal("found no Go packages — repoRoot is wrong, and every check below " +
			"would have passed vacuously")
	}
	return found
}

// packageComment returns the package doc comment for one file, or "".
func packageComment(t *testing.T, path string) string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if f.Doc == nil {
		return ""
	}
	return f.Doc.Text()
}

// TestEveryPackageIsDocumented is D1: a package a reader can open is a package
// that says what it is.
//
// The walk covers cmd/ as well as internal/ and contracts/. An executable is a
// package a reader opens like any other, and cmd/schemactl carries a chapter.
func TestEveryPackageIsDocumented(t *testing.T) {
	for _, p := range findPackages(t) {
		if len(p.goFiles) == 0 {
			// A test-only package cannot hold a doc.go: Go forbids a non-_test.go
			// file from declaring an external test package. It still has to be a
			// named decision, so an unlisted one fails here rather than passing
			// invisibly.
			if _, ok := exemptEntirely[p.path]; !ok {
				t.Errorf("%s: rule D1 — test-only package is not on the exempt list.\n"+
					"It cannot have a doc.go, so add it to exemptEntirely with a reason. "+
					"Skipping it silently would let any future test-only package escape "+
					"every check without anyone deciding that it should.", p.path)
			}
			continue
		}
		var documented bool
		for _, name := range p.goFiles {
			if strings.TrimSpace(packageComment(t, filepath.Join(p.dir, name))) != "" {
				documented = true
				break
			}
		}
		if !documented {
			t.Errorf("%s: rule D1 — no package comment anywhere in the package.\n"+
				"Add one. If this package carries a concept it needs a full chapter in "+
				"doc.go; see internal/platform/outbox/doc.go for the shape.", p.path)
		}
	}
}

// TestConceptBearingPackagesHaveAChapter is D2: the comment lives in doc.go, and
// doc.go holds nothing else.
func TestConceptBearingPackagesHaveAChapter(t *testing.T) {
	for _, p := range findPackages(t) {
		if !conceptBearing(p) {
			continue
		}
		docPath := filepath.Join(p.dir, "doc.go")
		if _, err := os.Stat(docPath); err != nil {
			t.Errorf("%s: rule D2 — no doc.go.\n"+
				"Every concept-bearing package keeps its package comment in doc.go. "+
				"Copy the shape from internal/platform/outbox/doc.go.\n"+
				"If this package carries no concept, add it to exemptFromChapter with a reason.",
				p.path)
			continue
		}
		if strings.TrimSpace(packageComment(t, docPath)) == "" {
			t.Errorf("%s: rule D2 — doc.go has no package comment.", p.path)
		}

		f, err := parser.ParseFile(token.NewFileSet(), docPath, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", docPath, err)
		}
		if len(f.Decls) != 0 || len(f.Imports) != 0 {
			t.Errorf("%s: rule D2 — doc.go holds %d declaration(s) and %d import(s).\n"+
				"doc.go holds the package comment and nothing else, so a reader opening it "+
				"gets the chapter and not a file to skim.", p.path, len(f.Decls), len(f.Imports))
		}
	}
}

// TestChaptersHaveTheSixHeadings is D3.
//
// It checks presence and order, not quality. The headings are the part a test
// can hold; whether "Why the obvious fixes do not work" actually refutes
// anything is a job for review.
func TestChaptersHaveTheSixHeadings(t *testing.T) {
	for _, p := range findPackages(t) {
		if !conceptBearing(p) {
			continue
		}
		docPath := filepath.Join(p.dir, "doc.go")
		if _, err := os.Stat(docPath); err != nil {
			continue // D2 already reported this
		}
		// Match whole lines, not substrings. godoc only treats "# Foo" as a
		// heading when it is a line of its own, so a chapter with all six crammed
		// into one paragraph would satisfy a substring check while rendering no
		// headings at all — and "# The problem with holds" would satisfy
		// "# The problem". Both were confirmed to slip through before this.
		lines := strings.Split(packageComment(t, docPath), "\n")
		lineOf := map[string]int{}
		for i, line := range lines {
			if h := strings.TrimSpace(line); strings.HasPrefix(h, "# ") {
				if _, seen := lineOf[h]; !seen {
					lineOf[h] = i
				}
			}
		}

		previous := -1
		for _, heading := range requiredHeadings {
			at, ok := lineOf[heading]
			if !ok {
				at = -1
			}
			if at < 0 {
				t.Errorf("%s: rule D3 — chapter is missing the heading %q.\n"+
					"All six headings from docs/conventions.md are required, in order. "+
					"See internal/platform/outbox/doc.go.", p.path, heading)
				continue
			}
			if at < previous {
				t.Errorf("%s: rule D3 — heading %q is out of order.\n"+
					"The six headings must appear in the order docs/conventions.md lists them.",
					p.path, heading)
			}
			previous = at
		}
	}
}

// TestNoSpecCitationsOutsideChapters is D4.
//
// A comment that cites a spec section is a footnote to a document the reader is
// not holding. Rule C6 gives citations exactly one home — the "Where this comes
// from" section of a chapter — and this is what keeps them there.
func TestNoSpecCitationsOutsideChapters(t *testing.T) {
	for _, p := range findPackages(t) {
		for _, name := range append(append([]string{}, p.goFiles...), p.testFiles...) {
			if name == "doc.go" {
				continue
			}
			path := filepath.Join(p.dir, name)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			if strings.Contains(string(body), sectionSign) {
				t.Errorf("%s/%s: rule D4 — contains a spec section reference.\n"+
					"Write out what the section says instead. Citations belong only under "+
					"'Where this comes from' in a doc.go.", p.path, name)
			}
		}
	}
}

// TestReadmeLinksEveryPlatformPackage is D5: the map stays complete as the
// territory grows.
func TestReadmeLinksEveryPlatformPackage(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join(repoRoot, "README.md"))
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot, "internal", "platform"))
	if err != nil {
		t.Fatalf("reading internal/platform: %v", err)
	}
	var checked int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		checked++
		link := "(internal/platform/" + e.Name() + ")"
		if !strings.Contains(string(readme), link) {
			t.Errorf("README.md: rule D5 — no link to internal/platform/%s.\n"+
				"Add a row to the concepts table so a reader can find it. "+
				"Expected the link target %q.", e.Name(), link)
		}
	}
	if checked == 0 {
		t.Fatal("found no packages under internal/platform — this check passed vacuously")
	}
}

// TestReadmeMapListsEveryInternalDirectory keeps the repository map honest.
//
// The map in README.md reads as an enumeration of internal/, so an omission is a
// factual error rather than an editorial trim — a reader counts six entries and
// concludes there are six. It has already gone stale twice: once for
// internal/toolchain, once for internal/docs. Hence a test rather than a habit.
func TestReadmeMapListsEveryInternalDirectory(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join(repoRoot, "README.md"))
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot, "internal"))
	if err != nil {
		t.Fatalf("reading internal: %v", err)
	}
	var checked int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		checked++
		if !strings.Contains(string(readme), "  "+e.Name()+"/") {
			t.Errorf("README.md: the repository map does not list internal/%s.\n"+
				"The map reads as an enumeration, so an omission tells the reader "+
				"something false about what is in the tree.", e.Name())
		}
	}
	if checked == 0 {
		t.Fatal("found no directories under internal — this check passed vacuously")
	}
}

// conceptBearing reports whether a package must carry a full chapter.
//
// Everything with real code does, except the two exempt lists. Defaulting to
// "yes" is deliberate: a package added later is included automatically, and
// excluding it takes a visible edit to exemptFromChapter with a reason.
func conceptBearing(p pkg) bool {
	if _, ok := exemptFromChapter[p.path]; ok {
		return false
	}
	// Test-only packages are handled by D1, which requires them to be listed in
	// exemptEntirely. They cannot carry a chapter, so they are not concept-bearing
	// here regardless.
	return len(p.goFiles) > 0
}

// TestExemptionsAreReal keeps the two exempt lists honest.
//
// An exemption for a package that no longer exists is a stale excuse that would
// silently start covering something else if the path were ever reused.
func TestExemptionsAreReal(t *testing.T) {
	present := map[string]bool{}
	for _, p := range findPackages(t) {
		present[p.path] = true
	}
	for _, list := range []map[string]string{exemptFromChapter, exemptEntirely} {
		for path, reason := range list {
			if !present[path] {
				t.Errorf("%s is exempt (%q) but no such package exists.\n"+
					"Remove the exemption — a stale one silently covers whatever takes the path next.",
					path, reason)
			}
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is exempt with no reason given.", path)
			}
		}
	}
}
