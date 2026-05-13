package resource

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestNoProviderImportsOrAllocationCalls(t *testing.T) {
	bannedCalls := map[string]bool{
		"OpenDevice":          true,
		"NewProtectionDomain": true,
		"RegisterMemory":      true,
		"NewQueuePair":        true,
		"NewCompletionQueue":  true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, file := range files {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range f.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "github.com/tmc/apple" || strings.HasPrefix(path, "github.com/tmc/apple/") || strings.Contains(path, "/internal/rdma") {
				t.Fatalf("%s imports provider package %q", file, path)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Ident:
				if bannedCalls[n.Name] {
					t.Fatalf("%s references provider allocation call %s", file, n.Name)
				}
			case *ast.SelectorExpr:
				if bannedCalls[n.Sel.Name] {
					t.Fatalf("%s references provider allocation call %s", file, n.Sel.Name)
				}
			}
			return true
		})
	}
}
