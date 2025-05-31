package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	outFileName        = "current-data"
	segmentPrefix      = "segment-"
	defaultMaxSegSize  = 10 * 1024 * 1024 // 10MB
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

// type segmentInfo struct { ???
// 	path   string
// 	offset int64
// }

type Db struct {
	dir           string
	out           *os.File
	outOffset     int64
	maxSegmentSize int64

	index    hashIndex
	segments []string // список файлів сегментів
	mu       sync.RWMutex
}

func Open(dir string) (*Db, error) {
	return OpenWithSegmentSize(dir, defaultMaxSegSize)
}

func OpenWithSegmentSize(dir string, maxSegmentSize int64) (*Db, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		dir:            dir,
		out:            f,
		maxSegmentSize: maxSegmentSize,
		index:          make(hashIndex),
		segments:       make([]string, 0),
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	return db, nil
}

func (db *Db) recover() error {
	// Спочатку знаходимо всі файли сегментів
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var segmentFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), segmentPrefix) {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}

	// Сортуємо сегменти за номером
	sort.Slice(segmentFiles, func(i, j int) bool {
		numI := extractSegmentNumber(segmentFiles[i])
		numJ := extractSegmentNumber(segmentFiles[j])
		return numI < numJ
	})

	db.segments = segmentFiles

	// Відновлюємо індекс з сегментів (від старіших до новіших)
	for _, segmentFile := range segmentFiles {
		err := db.recoverFromFile(filepath.Join(db.dir, segmentFile))
		if err != nil && err != io.EOF {
			return err
		}
	}

	// Відновлюємо індекс з поточного файлу
	err = db.recoverFromCurrentFile()
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (db *Db) recoverFromFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	var offset int64

	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file: %s", filePath)
			}
			break
		}
		if err != nil {
			return err
		}

		// Оновлюємо індекс (новіші записи перезаписують старіші)
		db.index[record.key] = offset
		offset += int64(n)
	}

	return nil
}

func (db *Db) recoverFromCurrentFile() error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	var offset int64

	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file: %s", db.out.Name())
			}
			break
		}
		if err != nil {
			return err
		}

		db.index[record.key] = offset
		offset += int64(n)
	}

	db.outOffset = offset
	return nil
}

func extractSegmentNumber(filename string) int {
	parts := strings.Split(filename, "-")
	if len(parts) < 2 {
		return 0
	}
	
	num, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0
	}
	return num
}

func (db *Db) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if db.out != nil {
		return db.out.Close()
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	position, ok := db.index[key]
	segments := make([]string, len(db.segments))
	copy(segments, db.segments)
	db.mu.RUnlock()

	if !ok {
		return "", ErrNotFound
	}

	// Спочатку шукаємо в поточному файлі
	value, err := db.getFromFile(db.out.Name(), position)
	if err == nil {
		return value, nil
	}

	// Якщо не знайшли в поточному файлі, шукаємо в сегментах
	for i := len(segments) - 1; i >= 0; i-- {
		segmentPath := filepath.Join(db.dir, segments[i])
		value, err := db.getFromFile(segmentPath, position)
		if err == nil {
			return value, nil
		}
	}

	return "", ErrNotFound
}

func (db *Db) getFromFile(filePath string, position int64) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	e := entry{
		key:   key,
		value: value,
	}

	// Перевіряємо, чи потрібно створити новий сегмент
	currentSize, err := db.getCurrentFileSize()
	if err != nil {
		return err
	}

	encodedEntry := e.Encode()
	if currentSize+int64(len(encodedEntry)) > db.maxSegmentSize {
		err := db.rotateSegment()
		if err != nil {
			return err
		}
	}

	n, err := db.out.Write(encodedEntry)
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) getCurrentFileSize() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) rotateSegment() error {
	// Закриваємо поточний файл
	if err := db.out.Close(); err != nil {
		return err
	}

	// Створюємо новий сегмент з поточного файлу
	segmentName := fmt.Sprintf("%s%d", segmentPrefix, time.Now().UnixNano())
	segmentPath := filepath.Join(db.dir, segmentName)
	currentPath := filepath.Join(db.dir, outFileName)

	// Перейменовуємо поточний файл в сегмент
	if err := os.Rename(currentPath, segmentPath); err != nil {
		return err
	}

	// Додаємо сегмент до списку
	db.segments = append(db.segments, segmentName)

	// Створюємо новий поточний файл
	f, err := os.OpenFile(currentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.out = f
	db.outOffset = 0

	return nil
}

func (db *Db) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) MergeSegments() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.segments) < 2 {
		return nil // Немає чого зливати
	}

	// Створюємо тимчасовий файл для злиття
	tempMergedPath := filepath.Join(db.dir, "temp-merged-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	tempFile, err := os.OpenFile(tempMergedPath, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	defer func() {
		tempFile.Close()
		os.Remove(tempMergedPath) // Видаляємо тимчасовий файл у випадку помилки
	}()

	// Збираємо всі унікальні ключі з їх найновішими значеннями
	mergedData := make(map[string]string)
	
	// Читаємо дані з сегментів (від старіших до новіших)
	for _, segmentFile := range db.segments {
		segmentPath := filepath.Join(db.dir, segmentFile)
		err := db.readSegmentIntoMap(segmentPath, mergedData)
		if err != nil {
			return err
		}
	}

	// Записуємо злиті дані у тимчасовий файл
	var offset int64
	newIndex := make(hashIndex)
	
	for key, value := range mergedData {
		entry := entry{key: key, value: value}
		encoded := entry.Encode()
		
		n, err := tempFile.Write(encoded)
		if err != nil {
			return err
		}
		
		newIndex[key] = offset
		offset += int64(n)
	}

	if err := tempFile.Sync(); err != nil {
		return err
	}
	tempFile.Close()

	// Атомарно замінюємо старі сегменти новим
	newSegmentName := fmt.Sprintf("%s%d", segmentPrefix, time.Now().UnixNano())
	newSegmentPath := filepath.Join(db.dir, newSegmentName)
	
	if err := os.Rename(tempMergedPath, newSegmentPath); err != nil {
		return err
	}

	// Видаляємо старі сегменти
	for _, segmentFile := range db.segments {
		segmentPath := filepath.Join(db.dir, segmentFile)
		os.Remove(segmentPath) // Ігноруємо помилки видалення
	}

	// Оновлюємо список сегментів
	db.segments = []string{newSegmentName}

	// Оновлюємо індекс з поточного файлу
	currentIndex := make(hashIndex)
	err = db.rebuildCurrentIndex(currentIndex)
	if err != nil {
		return err
	}

	// Об'єднуємо індекси (поточний файл має пріоритет)
	for key, position := range newIndex {
		if _, exists := currentIndex[key]; !exists {
			db.index[key] = position
		}
	}
	
	for key, position := range currentIndex {
		db.index[key] = position
	}

	return nil
}

func (db *Db) readSegmentIntoMap(segmentPath string, data map[string]string) error {
	file, err := os.Open(segmentPath)
	if err != nil {
		return err
	}
	defer file.Close()

	in := bufio.NewReader(file)
	for {
		var record entry
		_, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		
		data[record.key] = record.value
	}
	
	return nil
}

func (db *Db) rebuildCurrentIndex(index hashIndex) error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	var offset int64

	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		index[record.key] = offset
		offset += int64(n)
	}

	return nil
}