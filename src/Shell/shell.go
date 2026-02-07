package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func main() {
	cwd, _ := os.Getwd()
	fmt.Println("Emerald Shell")
	fmt.Println("cwd:", cwd)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("emer-shell> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			return
		}

		parts := splitArgs(line)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "touch":
			if len(parts) != 2 {
				fmt.Println("usage: touch <file>.emer")
				continue
			}
			if err := touch(parts[1]); err != nil {
				fmt.Println("error:", err)
			}
		case "carve":
			if len(parts) != 2 {
				fmt.Println("usage: carve <file>.emer")
				continue
			}
			if err := carve(parts[1]); err != nil {
				fmt.Println("error:", err)
			}
		case "shine":
			if len(parts) != 2 {
				fmt.Println("usage: shine <file>.emer")
				continue
			}
			if err := shine(parts[1]); err != nil {
				fmt.Println("error:", err)
			}
		default:
			fmt.Println("unknown command:", parts[0])
		}
	}
}

func touch(path string) error {
	if !strings.HasSuffix(strings.ToLower(path), ".emer") {
		return fmt.Errorf("file must end with .emer")
	}
	f, err := os.OpenFile(path, os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return f.Close()
}

func carve(path string) error {
	if !strings.HasSuffix(strings.ToLower(path), ".emer") {
		return fmt.Errorf("file must end with .emer")
	}
	editor := os.Getenv("EDITOR")
	if editor == "" {
		if runtime.GOOS == "windows" {
			editor = "notepad"
		} else {
			editor = "vi"
		}
	}
	cmd := exec.Command(editor, path)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func shine(path string) error {
	if !strings.HasSuffix(strings.ToLower(path), ".emer") {
		return fmt.Errorf("file must end with .emer")
	}

	emeraldc := filepath.Join("..", "emeraldc")
	if runtime.GOOS == "windows" {
		emeraldc += ".exe"
	}

	if _, err := os.Stat(emeraldc); err == nil {
		if err := runCmd(emeraldc, "build", path); err != nil {
			return err
		}
		emec := strings.TrimSuffix(path, filepath.Ext(path)) + ".emec"
		return runCmd(emeraldc, "run", emec)
	}

	mainGo := filepath.Join("..", "main.go")

	if err := runCmd("go", "run", mainGo, "build", path); err != nil {
		return err
	}
	emec := strings.TrimSuffix(path, filepath.Ext(path)) + ".emec"
	return runCmd("go", "run", mainGo, "run", emec)
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func splitArgs(s string) []string {
	out := []string{}
	var cur strings.Builder
	inQuote := false
	var quote rune

	for _, r := range s {
		if inQuote {
			if r == quote {
				inQuote = false
			} else {
				cur.WriteRune(r)
			}
			continue
		}
		switch r {
		case '"', '\'':
			inQuote = true
			quote = r
		case ' ', '\t':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}
