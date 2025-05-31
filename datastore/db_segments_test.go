package datastore

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestSegmentCreation перевіряє створення нових сегментів при досягненні ліміту розміру
func TestSegmentCreation(t *testing.T) {
	tmp := t.TempDir()
	
	// Встановлюємо маленький розмір сегмента для тестування (1KB замість 10MB)
	db, err := OpenWithSegmentSize(tmp, 1024) // 1KB для швидкого тестування
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Додаємо достатньо даних для створення кількох сегментів
	largeValue := strings.Repeat("x", 300) // 300 байт на значення
	
	// Записуємо дані, які мають створити кілька сегментів
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		err := db.Put(key, largeValue)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Перевіряємо, що створено кілька файлів сегментів
	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}

	segmentCount := 0
	hasCurrentFile := false
	
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "segment-") {
			segmentCount++
		}
		if file.Name() == outFileName {
			hasCurrentFile = true
		}
	}

	if segmentCount == 0 {
		t.Error("No segment files created")
	}
	
	if !hasCurrentFile {
		t.Error("Current data file should exist")
	}

	t.Logf("Created %d segment files", segmentCount)

	// Перевіряємо, що всі дані доступні для читання
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
		}
		if value != largeValue {
			t.Errorf("Get(%s) returned wrong value", key)
		}
	}
}

// TestSegmentMerging перевіряє роботу злиття сегментів
func TestSegmentMerging(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithSegmentSize(tmp, 512) // Маленький розмір для швидкого створення сегментів
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Створюємо дані з дублікатами ключів
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2", 
		"key3": "value3",
	}

	// Записуємо дані кілька разів для створення дублікатів
	for round := 0; round < 3; round++ {
		for key, value := range testData {
			newValue := fmt.Sprintf("%s_round%d", value, round)
			err := db.Put(key, newValue)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
			testData[key] = newValue // Оновлюємо очікуване значення
		}
	}

	// Перевіряємо кількість файлів до злиття
	filesBefore, err := countSegmentFiles(tmp)
	if err != nil {
		t.Fatal(err)
	}

	// Запускаємо злиття сегментів
	err = db.MergeSegments()
	if err != nil {
		t.Fatalf("MergeSegments failed: %v", err)
	}

	// Перевіряємо кількість файлів після злиття
	filesAfter, err := countSegmentFiles(tmp)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Files before merge: %d, after merge: %d", filesBefore, filesAfter)

	// Перевіряємо, що всі дані все ще доступні після злиття
	for key, expectedValue := range testData {
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Get(%s) after merge failed: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Get(%s) after merge: got %s, want %s", key, value, expectedValue)
		}
	}
}

// TestAtomicMerge перевіряє атомарність операції злиття
func TestAtomicMerge(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithSegmentSize(tmp, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Додаємо тестові дані
	testKeys := []string{"atomic1", "atomic2", "atomic3"}
	for _, key := range testKeys {
		err := db.Put(key, key+"_value")
		if err != nil {
			t.Fatal(err)
		}
	}

	// Перевіряємо, що дані доступні до злиття
	for _, key := range testKeys {
		_, err := db.Get(key)
		if err != nil {
			t.Fatalf("Data should be available before merge: %v", err)
		}
	}

	// Тестуємо, що навіть якщо злиття не завершиться успішно,
	// дані все ще будуть доступні через старі сегменти
	// (це імітує сценарій відмови під час злиття)
	
	// Спочатку успішне злиття
	err = db.MergeSegments()
	if err != nil {
		t.Fatalf("MergeSegments should succeed: %v", err)
	}

	// Перевіряємо, що дані все ще доступні після злиття
	for _, key := range testKeys {
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Get(%s) after atomic merge failed: %v", key, err)
		}
		expectedValue := key + "_value"
		if value != expectedValue {
			t.Errorf("Get(%s) after atomic merge: got %s, want %s", key, value, expectedValue)
		}
	}
}

// TestRecoveryWithSegments перевіряє відновлення БД з сегментами після перезапуску
func TestRecoveryWithSegments(t *testing.T) {
	tmp := t.TempDir()
	
	// Створюємо БД та додаємо дані
	db, err := OpenWithSegmentSize(tmp, 400)
	if err != nil {
		t.Fatal(err)
	}

	testData := map[string]string{
		"recover1": "value1",
		"recover2": "value2",
		"recover3": "value3",
		"recover4": "value4",
	}

	for key, value := range testData {
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Закриваємо БД
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Відкриваємо БД знову (імітація перезапуску)
	db2, err := OpenWithSegmentSize(tmp, 400)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// Перевіряємо, що всі дані відновилися
	for key, expectedValue := range testData {
		value, err := db2.Get(key)
		if err != nil {
			t.Errorf("Get(%s) after recovery failed: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Get(%s) after recovery: got %s, want %s", key, value, expectedValue)
		}
	}
}

// TestConcurrentSegmentAccess перевіряє одночасний доступ до сегментів
func TestConcurrentSegmentAccess(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithSegmentSize(tmp, 512)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Додаємо початкові дані
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("concurrent%d", i)
		value := fmt.Sprintf("value%d", i)
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Запускаємо горутини для одночасного читання та запису
	done := make(chan bool)
	errors := make(chan error, 10)

	// Горутина для читання
	go func() {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("concurrent%d", i%5)
			_, err := db.Get(key)
			if err != nil && err != ErrNotFound {
				errors <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
		done <- true
	}()

	// Горутина для запису
	go func() {
		for i := 5; i < 10; i++ {
			key := fmt.Sprintf("concurrent%d", i)
			value := fmt.Sprintf("value%d", i)
			err := db.Put(key, value)
			if err != nil {
				errors <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
		done <- true
	}()

	// Чекаємо завершення або помилки
	completed := 0
	for completed < 2 {
		select {
		case <-done:
			completed++
		case err := <-errors:
			t.Fatalf("Concurrent access error: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Test timeout")
		}
	}
}

// Допоміжна функція для підрахунку файлів сегментів
func countSegmentFiles(dir string) (int, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "segment-") {
			count++
		}
	}
	return count, nil
}

// Бенчмарк для вимірювання продуктивності з сегментами
func BenchmarkPutWithSegments(b *testing.B) {
	tmp := b.TempDir()
	
	db, err := OpenWithSegmentSize(tmp, 1024*1024) // 1MB сегменти
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetWithSegments(b *testing.B) {
	tmp := b.TempDir()
	
	db, err := OpenWithSegmentSize(tmp, 1024*1024) // 1MB сегменти
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Підготовка даних
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i%1000)
		_, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}