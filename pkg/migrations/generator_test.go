package migrations

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratePostgres(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "test_migration.sql",
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify schema creation
	if !strings.Contains(sql, "CREATE SCHEMA IF NOT EXISTS orchestrator") {
		t.Error("Missing schema creation")
	}

	// Verify projection_shards table
	requiredProjectionShardsStrings := []string{
		"CREATE TABLE IF NOT EXISTS orchestrator.projection_shards",
		"projection_name TEXT NOT NULL",
		"shard_id INT NOT NULL",
		"shard_count INT NOT NULL",
		"last_global_position BIGINT NOT NULL DEFAULT 0",
		"owner_id TEXT",
		"state TEXT NOT NULL DEFAULT 'idle'",
		"CHECK (state IN ('idle', 'assigned', 'draining'))",
		"updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"PRIMARY KEY (projection_name, shard_id)",
		"CHECK (shard_id >= 0 AND shard_id < shard_count)",
	}

	for _, required := range requiredProjectionShardsStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("projection_shards table missing required string: %s", required)
		}
	}

	// Verify recreate_lock table
	requiredRecreateLockStrings := []string{
		"CREATE TABLE IF NOT EXISTS orchestrator.recreate_lock",
		"lock_id INT PRIMARY KEY DEFAULT 1 CHECK (lock_id = 1)",
		"generation BIGINT NOT NULL DEFAULT 0",
		"phase TEXT NOT NULL DEFAULT 'idle'",
		"CHECK (phase IN ('idle', 'draining', 'assigning', 'running'))",
		"updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"INSERT INTO orchestrator.recreate_lock",
		"VALUES (1, 0, 'idle', NOW())",
		"ON CONFLICT (lock_id) DO NOTHING",
	}

	for _, required := range requiredRecreateLockStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("recreate_lock table missing required string: %s", required)
		}
	}

	// Verify workers table
	requiredWorkersStrings := []string{
		"CREATE TABLE IF NOT EXISTS orchestrator.workers",
		"worker_id TEXT PRIMARY KEY",
		"generation_seen BIGINT NOT NULL DEFAULT 0",
		"status TEXT NOT NULL DEFAULT 'starting'",
		"CHECK (status IN ('starting', 'ready', 'running', 'draining', 'stopped'))",
		"last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()",
	}

	for _, required := range requiredWorkersStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("workers table missing required string: %s", required)
		}
	}

	// Verify indexes are created
	requiredIndexes := []string{
		"idx_projection_shards_state",
		"idx_projection_shards_owner",
		"idx_projection_shards_updated",
		"idx_recreate_lock_updated",
		"idx_workers_status",
		"idx_workers_heartbeat",
		"idx_workers_generation",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}
}

func TestGeneratePostgres_CustomNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "custom_migration.sql",
		SchemaName:            "custom_schema",
		ProjectionShardsTable: "custom_shards",
		RecreateLockTable:     "custom_lock",
		WorkersTable:          "custom_workers",
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify custom names are used
	if !strings.Contains(sql, "CREATE SCHEMA IF NOT EXISTS custom_schema") {
		t.Error("Custom schema name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_schema.custom_shards") {
		t.Error("Custom projection shards table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_schema.custom_lock") {
		t.Error("Custom recreate lock table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_schema.custom_workers") {
		t.Error("Custom workers table name not used")
	}
}

func TestGenerateMySQL(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "test_migration.sql",
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	err := GenerateMySQL(&config)
	if err != nil {
		t.Fatalf("GenerateMySQL failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify database creation
	if !strings.Contains(sql, "CREATE DATABASE IF NOT EXISTS orchestrator") {
		t.Error("Missing database creation")
	}
	if !strings.Contains(sql, "USE orchestrator") {
		t.Error("Missing USE database statement")
	}

	// Verify projection_shards table for MySQL
	requiredProjectionShardsStrings := []string{
		"CREATE TABLE IF NOT EXISTS projection_shards",
		"projection_name VARCHAR(255) NOT NULL",
		"shard_id INT NOT NULL",
		"shard_count INT NOT NULL",
		"last_global_position BIGINT NOT NULL DEFAULT 0",
		"owner_id VARCHAR(255)",
		"state ENUM('idle', 'assigned', 'draining') NOT NULL DEFAULT 'idle'",
		"updated_at TIMESTAMP(6) NOT NULL",
		"PRIMARY KEY (projection_name, shard_id)",
		"CHECK (shard_id >= 0 AND shard_id < shard_count)",
		"ENGINE=InnoDB",
		"CHARSET=utf8mb4",
	}

	for _, required := range requiredProjectionShardsStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("projection_shards table missing required string: %s", required)
		}
	}

	// Verify recreate_lock table
	requiredRecreateLockStrings := []string{
		"CREATE TABLE IF NOT EXISTS recreate_lock",
		"lock_id INT PRIMARY KEY DEFAULT 1",
		"generation BIGINT NOT NULL DEFAULT 0",
		"phase ENUM('idle', 'draining', 'assigning', 'running') NOT NULL DEFAULT 'idle'",
		"updated_at TIMESTAMP(6) NOT NULL",
		"CHECK (lock_id = 1)",
		"INSERT INTO recreate_lock",
		"VALUES (1, 0, 'idle',",
		"ON DUPLICATE KEY UPDATE lock_id = lock_id",
	}

	for _, required := range requiredRecreateLockStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("recreate_lock table missing required string: %s", required)
		}
	}

	// Verify workers table
	requiredWorkersStrings := []string{
		"CREATE TABLE IF NOT EXISTS workers",
		"worker_id VARCHAR(255) PRIMARY KEY",
		"generation_seen BIGINT NOT NULL DEFAULT 0",
		"status ENUM('starting', 'ready', 'running', 'draining', 'stopped') NOT NULL DEFAULT 'starting'",
		"last_heartbeat TIMESTAMP(6) NOT NULL",
	}

	for _, required := range requiredWorkersStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("workers table missing required string: %s", required)
		}
	}

	// Verify indexes
	requiredIndexes := []string{
		"idx_projection_shards_state",
		"idx_projection_shards_owner",
		"idx_projection_shards_updated",
		"idx_recreate_lock_updated",
		"idx_workers_status",
		"idx_workers_heartbeat",
		"idx_workers_generation",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}
}

func TestGenerateMySQL_CustomNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "custom_migration.sql",
		SchemaName:            "custom_db",
		ProjectionShardsTable: "custom_shards",
		RecreateLockTable:     "custom_lock",
		WorkersTable:          "custom_workers",
	}

	err := GenerateMySQL(&config)
	if err != nil {
		t.Fatalf("GenerateMySQL failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify custom names are used
	if !strings.Contains(sql, "CREATE DATABASE IF NOT EXISTS custom_db") {
		t.Error("Custom database name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_shards") {
		t.Error("Custom projection shards table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_lock") {
		t.Error("Custom recreate lock table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_workers") {
		t.Error("Custom workers table name not used")
	}
}

func TestGenerateSQLite(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "test_migration.sql",
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	err := GenerateSQLite(&config)
	if err != nil {
		t.Fatalf("GenerateSQLite failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify projection_shards table for SQLite (with prefix)
	requiredProjectionShardsStrings := []string{
		"CREATE TABLE IF NOT EXISTS orchestrator_projection_shards",
		"projection_name TEXT NOT NULL",
		"shard_id INTEGER NOT NULL",
		"shard_count INTEGER NOT NULL",
		"last_global_position INTEGER NOT NULL DEFAULT 0",
		"owner_id TEXT",
		"state TEXT NOT NULL DEFAULT 'idle'",
		"CHECK (state IN ('idle', 'assigned', 'draining'))",
		"updated_at TEXT NOT NULL",
		"PRIMARY KEY (projection_name, shard_id)",
		"CHECK (shard_id >= 0 AND shard_id < shard_count)",
	}

	for _, required := range requiredProjectionShardsStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("projection_shards table missing required string: %s", required)
		}
	}

	// Verify recreate_lock table
	requiredRecreateLockStrings := []string{
		"CREATE TABLE IF NOT EXISTS orchestrator_recreate_lock",
		"lock_id INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock_id = 1)",
		"generation INTEGER NOT NULL DEFAULT 0",
		"phase TEXT NOT NULL DEFAULT 'idle'",
		"CHECK (phase IN ('idle', 'draining', 'assigning', 'running'))",
		"updated_at TEXT NOT NULL",
		"INSERT OR IGNORE INTO orchestrator_recreate_lock",
		"VALUES (1, 0, 'idle', datetime('now'))",
	}

	for _, required := range requiredRecreateLockStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("recreate_lock table missing required string: %s", required)
		}
	}

	// Verify workers table
	requiredWorkersStrings := []string{
		"CREATE TABLE IF NOT EXISTS orchestrator_workers",
		"worker_id TEXT PRIMARY KEY",
		"generation_seen INTEGER NOT NULL DEFAULT 0",
		"status TEXT NOT NULL DEFAULT 'starting'",
		"CHECK (status IN ('starting', 'ready', 'running', 'draining', 'stopped'))",
		"last_heartbeat TEXT NOT NULL",
	}

	for _, required := range requiredWorkersStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("workers table missing required string: %s", required)
		}
	}

	// Verify indexes (with table prefix)
	requiredIndexes := []string{
		"idx_orchestrator_projection_shards_state",
		"idx_orchestrator_projection_shards_owner",
		"idx_orchestrator_projection_shards_updated",
		"idx_orchestrator_recreate_lock_updated",
		"idx_orchestrator_workers_status",
		"idx_orchestrator_workers_heartbeat",
		"idx_orchestrator_workers_generation",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}
}

func TestGenerateSQLite_CustomNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "custom_migration.sql",
		SchemaName:            "custom",
		ProjectionShardsTable: "custom_shards",
		RecreateLockTable:     "custom_lock",
		WorkersTable:          "custom_workers",
	}

	err := GenerateSQLite(&config)
	if err != nil {
		t.Fatalf("GenerateSQLite failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify custom names are used (with schema prefix)
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_custom_shards") {
		t.Error("Custom projection shards table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_custom_lock") {
		t.Error("Custom recreate lock table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_custom_workers") {
		t.Error("Custom workers table name not used")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// Verify defaults
	if config.OutputFolder != "migrations" {
		t.Errorf("Expected OutputFolder to be 'migrations', got '%s'", config.OutputFolder)
	}
	if config.SchemaName != "orchestrator" {
		t.Errorf("Expected SchemaName to be 'orchestrator', got '%s'", config.SchemaName)
	}
	if config.ProjectionShardsTable != "projection_shards" {
		t.Errorf("Expected ProjectionShardsTable to be 'projection_shards', got '%s'", config.ProjectionShardsTable)
	}
	if config.RecreateLockTable != "recreate_lock" {
		t.Errorf("Expected RecreateLockTable to be 'recreate_lock', got '%s'", config.RecreateLockTable)
	}
	if config.WorkersTable != "workers" {
		t.Errorf("Expected WorkersTable to be 'workers', got '%s'", config.WorkersTable)
	}

	// Verify filename has timestamp format
	if !strings.HasSuffix(config.OutputFilename, "_init_orchestrator_coordination.sql") {
		t.Errorf("Expected OutputFilename to end with '_init_orchestrator_coordination.sql', got '%s'", config.OutputFilename)
	}
}

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		fieldName string
		wantError bool
	}{
		{"valid simple", "table_name", "TableName", false},
		{"valid with numbers", "table123", "TableName", false},
		{"valid with underscores", "my_table_name", "TableName", false},
		{"empty string", "", "TableName", true},
		{"starts with number", "123table", "TableName", true},
		{"contains spaces", "table name", "TableName", true},
		{"contains dash", "table-name", "TableName", true},
		{"contains semicolon", "table;DROP TABLE users", "TableName", true},
		{"contains quotes", "table'name", "TableName", true},
		{"sql injection attempt", "table; DROP TABLE users--", "TableName", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.value, tt.fieldName)
			if tt.wantError && err == nil {
				t.Errorf("Expected error for value '%s', got nil", tt.value)
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error for value '%s', got: %v", tt.value, err)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		wantError bool
	}{
		{
			name: "valid config",
			config: Config{
				SchemaName:            "orchestrator",
				ProjectionShardsTable: "projection_shards",
				RecreateLockTable:     "recreate_lock",
				WorkersTable:          "workers",
			},
			wantError: false,
		},
		{
			name: "invalid schema name",
			config: Config{
				SchemaName:            "schema; DROP TABLE users--",
				ProjectionShardsTable: "projection_shards",
				RecreateLockTable:     "recreate_lock",
				WorkersTable:          "workers",
			},
			wantError: true,
		},
		{
			name: "invalid projection shards table",
			config: Config{
				SchemaName:            "orchestrator",
				ProjectionShardsTable: "table'; DROP TABLE users--",
				RecreateLockTable:     "recreate_lock",
				WorkersTable:          "workers",
			},
			wantError: true,
		},
		{
			name: "empty schema name",
			config: Config{
				SchemaName:            "",
				ProjectionShardsTable: "projection_shards",
				RecreateLockTable:     "recreate_lock",
				WorkersTable:          "workers",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(&tt.config)
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestGeneratePostgres_InvalidConfig(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "test.sql",
		SchemaName:            "schema'; DROP TABLE users--",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	err := GeneratePostgres(&config)
	if err == nil {
		t.Fatal("Expected error for invalid schema name, got nil")
	}
	if !strings.Contains(err.Error(), "invalid configuration") {
		t.Errorf("Expected error to mention 'invalid configuration', got: %v", err)
	}
}

func TestGenerateMySQL_InvalidConfig(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "test.sql",
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "table'; DROP TABLE users--",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	err := GenerateMySQL(&config)
	if err == nil {
		t.Fatal("Expected error for invalid table name, got nil")
	}
	if !strings.Contains(err.Error(), "invalid configuration") {
		t.Errorf("Expected error to mention 'invalid configuration', got: %v", err)
	}
}

func TestGenerateSQLite_InvalidConfig(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "test.sql",
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers'; DROP TABLE users--",
	}

	err := GenerateSQLite(&config)
	if err == nil {
		t.Fatal("Expected error for invalid workers table name, got nil")
	}
	if !strings.Contains(err.Error(), "invalid configuration") {
		t.Errorf("Expected error to mention 'invalid configuration', got: %v", err)
	}
}
