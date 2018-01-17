package common

import (
	"os"
	"path"
	"os/exec"
	"path/filepath"
)

// absolute path of exec file dir
func AbsExecDir() string {
	file, _ := exec.LookPath(os.Args[0])
	dir, _ := path.Split(file)
	abs_path, _ := filepath.Abs(dir)
	return abs_path
}
