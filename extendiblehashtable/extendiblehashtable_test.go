package extendiblehashtable

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pjimming/HermesDB/define"
)

func TestSampleEHT(t *testing.T) {
	ast := assert.New(t)

	table := NewExtendibleHashTable(2)
	table.Insert("1", "a")
	table.Insert("2", "b")
	table.Insert("3", "c")
	table.Insert("4", "d")
	table.Insert("5", "e")
	table.Insert("6", "f")
	table.Insert("7", "g")
	table.Insert("8", "h")
	table.Insert("9", "i")
	//ast.Equal(define.SizeT(2), table.GetLocalDepth(0))
	//ast.Equal(define.SizeT(3), table.GetLocalDepth(1))
	//ast.Equal(define.SizeT(2), table.GetLocalDepth(2))
	//ast.Equal(define.SizeT(2), table.GetLocalDepth(3))

	var result any
	var isFind bool
	result, _ = table.Find("9")
	ast.Equal("i", result)
	result, _ = table.Find("8")
	ast.Equal("h", result)
	result, _ = table.Find("2")
	ast.Equal("b", result)

	_, isFind = table.Find("10")
	ast.False(isFind)

	ast.True(table.Remove("8"))
	ast.True(table.Remove("4"))
	ast.True(table.Remove("1"))
	ast.False(table.Remove("20"))
}

func TestConcurrentInsert(t *testing.T) {
	ast := assert.New(t)

	numRuns := 50
	numThreads := 3

	// Run concurrent test multiple times to guarantee correctness.
	for run := 0; run < numRuns; run++ {
		table := NewExtendibleHashTable(2)

		var wg sync.WaitGroup
		for tid := 0; tid < numThreads; tid++ {
			i := tid
			wg.Add(1)
			go func() {
				defer wg.Done()
				table.Insert(strconv.Itoa(i), i)
			}()
		}
		wg.Wait()

		ast.Equal(define.SizeT(1), table.GetGlobalDepth())
		for tid := 0; tid < numThreads; tid++ {
			result, isFind := table.Find(strconv.Itoa(tid))
			ast.True(isFind)
			ast.Equal(tid, result)
		}
	}
}
