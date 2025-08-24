package db

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

const defaultSize = 8

func NewManifest(fileName string) *Manifest {
	mf := &Manifest{
		Name:  fileName,
		Files: make([]string, 0, defaultSize),
		lock:  sync.Mutex{},
	}
	if err := mf.init(); err != nil {
		return nil
	}
	return mf
}

type Manifest struct {
	Name  string
	Files []string
	lock  sync.Mutex
}

func (f *Manifest) readFile() ([]byte, error) {
	if file, err := os.OpenFile(f.Name, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, err
	} else {
		defer file.Close()
		return io.ReadAll(file)
	}
}

func (f *Manifest) init() error {
	if d, err := f.readFile(); err != nil {
		return err
	} else {
		if len(d) == 0 {
			if err := f.write(fmt.Sprintf("%08d", 1)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Manifest) Load() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Files = []string{}
	if data, err := f.readFile(); err != nil {
		return err
	} else {
		arr := strings.Split(string(data), "\n")
		if len(arr) == 0 {
			return nil
		}
		f.Name = arr[0]
		for _, v := range arr[1:] {
			f.Files = append(f.Files, v)
		}
	}
	return nil
}

func (f *Manifest) flush() error {
	// 打开文件，如果文件不存在则创建
	file, err := os.OpenFile(f.Name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create file: %w", err)
	}
	defer file.Close()
	// 写入内容
	if _, err := io.WriteString(file, f.String()); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}
	return nil
}
func (f *Manifest) String() string {
	var r string
	r += f.Name + "\n"
	for i, v := range f.Files {
		if i != len(f.Files)-1 {
			r += v + "\n"
		} else {
			r += v
		}
	}
	return r
}

func (f *Manifest) write(fileName string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Files = append(f.Files, fileName)
	return f.flush()
}

func (f *Manifest) NewFile() string {
	f.lock.Lock()
	defer f.lock.Unlock()
	fileName := f.Files[len(f.Files)-1]
	i, _ := strconv.Atoi(fileName)
	newFile := fmt.Sprintf("%08d", i+1)
	f.Files = append(f.Files, newFile)
	_ = f.flush()
	return newFile
}

func (f *Manifest) Delete(name string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	result := make([]string, 0, 4)
	for _, v := range f.Files {
		if v != name {
			result = append(result, v)
		}
	}
	f.Files = result
	return f.flush()
}
