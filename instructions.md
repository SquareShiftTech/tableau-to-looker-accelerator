# Coding Instructions for Claude - Tableau to LookML Migration Library

## **Overview for Claude**

You will develop a Tableau to LookML migration library using a **plugin architecture**. Each phase builds incrementally on the previous one. Focus on **production-ready code** with comprehensive testing.

---

## **Phase 1: Foundation & Core Architecture (Weeks 1-2)**

### **Phase 1.1: Core Architecture Setup**

**Task 1: Create Plugin Registry System**
```
Create: tableau_looker_lib/core/plugin_registry.py

Requirements:
- PluginRegistry class with handler management
- Methods: register_handler(handler, priority), get_handler(element), register_fallback(handler)
- Priority-based handler selection (lower number = higher priority)
- Support for fallback handlers when no match found
- Comprehensive error handling and logging
- Thread-safe implementation

Include complete unit tests in: tests/test_plugin_registry.py
Test: Handler registration, priority ordering, fallback selection, thread safety
```

**Task 2: Create Base Handler Abstract Class**
```
Create: tableau_looker_lib/handlers/base_handler.py

Requirements:
- BaseHandler abstract class with template method pattern
- Abstract methods: can_handle(element), extract(element), convert_to_json(data)
- Concrete methods: process(element), calculate_confidence(data), validate_input(element)
- Confidence scoring system (0.0-1.0)
- Error handling with detailed error messages
- Logging integration

Include complete unit tests in: tests/test_base_handler.py
Test: Abstract method enforcement, confidence calculation, error handling
```

**Task 3: Create Migration Engine**
```
Create: tableau_looker_lib/core/migration_engine.py

Requirements:
- MigrationEngine class orchestrating entire conversion process
- Methods: migrate_file(tableau_file, output_dir), register_custom_handler(handler)
- Integration with plugin registry
- XML → JSON → LookML pipeline
- Comprehensive error handling and recovery
- Progress tracking and logging

Include complete unit tests in: tests/test_migration_engine.py
Test: End-to-end migration, plugin integration, error recovery
```

### **Phase 1.2: Basic Handlers**

**Task 4: Create Connection Handler**
```
Create: tableau_looker_lib/handlers/connection_handler.py

Requirements:
- ConnectionHandler class extending BaseHandler
- Support: PostgreSQL, MySQL, SQL Server, Oracle, BigQuery, Snowflake
- Extract: server, database, port, username, schema, SSL settings
- Convert to JSON intermediate format with proper structure
- Confidence scoring based on connection type support
- Generate clean LookML connection names

Include complete unit tests in: tests/test_connection_handler.py
Test: All supported database types, connection string parsing, confidence scoring
Create test fixtures for each database type
```

**Task 5: Create Dimension Handler**
```
Create: tableau_looker_lib/handlers/dimension_handler.py

Requirements:
- DimensionHandler class extending BaseHandler
- Support: string, integer, real, boolean, date, datetime dimensions
- Field name cleaning: [Field Name] → field_name
- Date dimension groups with proper timeframes
- Handle hidden fields and captions
- Data type mapping to LookML types

Include complete unit tests in: tests/test_dimension_handler.py
Test: All data types, field name cleaning, date dimensions, hidden fields
```

**Task 6: Create Measure Handler**
```
Create: tableau_looker_lib/handlers/measure_handler.py

Requirements:
- MeasureHandler class extending BaseHandler
- Support: SUM, COUNT, AVG, MIN, MAX aggregations
- Value formatting (currency, percentage, decimal)
- Drill-down capabilities
- Proper measure type mapping

Include complete unit tests in: tests/test_measure_handler.py
Test: All aggregation types, value formatting, drill-down
```

**Task 7: Create Fallback Handler**
```
Create: tableau_looker_lib/handlers/fallback_handler.py

Requirements:
- FallbackHandler class extending BaseHandler
- Handle ANY unknown element gracefully
- Extract basic element information (tag, attributes, text, children)
- Generate manual review items
- Track unknown elements for analysis
- Always return low confidence (0.1)

Include complete unit tests in: tests/test_fallback_handler.py
Test: Unknown element handling, manual review generation, element tracking
```

### **Phase 1.3: JSON Schema & Basic Generation**

**Task 8: Create JSON Schema and Models**
```
Create: tableau_looker_lib/models/json_schema.py
Create: tableau_looker_lib/models/migration_result.py

Requirements:
- Complete JSON intermediate format schema
- Validation classes for schema compliance
- MigrationResult class with success/failure tracking
- Error collection and reporting
- Element linking and reference validation

Include complete unit tests in: tests/test_json_schema.py
Test: Schema validation, reference integrity, error collection
```

**Task 9: Create XML Parser**
```
Create: tableau_looker_lib/core/xml_parser.py

Requirements:
- TableauXMLParser class for .twb and .twbx files
- Handle both file types (extract .twb from .twbx)
- Parse XML into structured data
- Error handling for malformed XML
- Support for large files

Include complete unit tests in: tests/test_xml_parser.py
Test: Both file types, XML parsing, error handling, large files
```

**Task 10: Create Basic LookML Generator**
```
Create: tableau_looker_lib/generators/lookml_generator.py
Create: tableau_looker_lib/generators/template_engine.py

Requirements:
- LookMLGenerator class for basic file generation
- TemplateEngine using Jinja2 templates
- Generate: connection.lkml, basic view.lkml files
- File organization and naming conventions
- Template loading and variable substitution

Create basic templates in: tableau_looker_lib/templates/
- connection.j2 - Connection template
- basic_view.j2 - Basic view template

Include complete unit tests in: tests/test_lookml_generator.py
Test: Template rendering, file generation, variable substitution
```

### **Phase 1 Integration Testing**

**Task 11: Create Integration Test Suite**
```
Create: tests/integration/test_phase1_integration.py

Requirements:
- End-to-end tests using sample Tableau files
- Test complete XML → JSON → LookML pipeline
- Verify all handlers work together
- Test plugin registry integration
- Performance benchmarks

Create test fixtures: tests/fixtures/
- sample_basic.twb - Basic Tableau workbook
- sample_connections.twb - Multiple connection types
- expected_output/ - Expected LookML files

Test: Complete migration pipeline, handler integration, performance
```

---

## **Phase 2: Advanced Handlers & Business Logic (Weeks 3-4)**

### **Phase 2.1: Calculated Field Handler**

**Task 12: Create Formula Converter**
```
Create: tableau_looker_lib/converters/formula_converter.py

Requirements:
- FormulaConverter class with pattern-based conversion
- Support 50+ Tableau functions (IF, CASE, string, date, math)
- Pattern system: [Field Name] → ${field_name}
- Division by zero protection with NULLIF
- Field dependency extraction
- Formula complexity analysis (Simple/Medium/Complex)

Include complete unit tests in: tests/test_formula_converter.py
Test: All supported functions, pattern matching, dependency extraction
Create comprehensive test cases for each function type
```

**Task 13: Create Calculated Field Handler**
```
Create: tableau_looker_lib/handlers/calculated_field_handler.py

Requirements:
- CalculatedFieldHandler class extending BaseHandler
- Integration with FormulaConverter
- Handle complex patterns: LOD expressions, table calculations
- Generate proper LookML dimensions/measures
- Confidence scoring based on formula complexity

Include complete unit tests in: tests/test_calculated_field_handler.py
Test: Formula conversion accuracy, complex patterns, confidence scoring
```

### **Phase 2.2: Custom SQL & Parameters**

**Task 14: Create Custom SQL Handler**
```
Create: tableau_looker_lib/handlers/custom_sql_handler.py

Requirements:
- CustomSQLHandler class extending BaseHandler
- Parse custom SQL relations
- Generate LookML derived tables
- Field detection from SQL
- SQL validation and optimization hints

Include complete unit tests in: tests/test_custom_sql_handler.py
Test: SQL parsing, field detection, derived table generation
```

**Task 15: Create Parameter Handler**
```
Create: tableau_looker_lib/handlers/parameter_handler.py

Requirements:
- ParameterHandler class extending BaseHandler
- Support: string, number, date parameters
- Handle allowed values and defaults
- Type conversion and validation
- Generate proper LookML parameter blocks

Include complete unit tests in: tests/test_parameter_handler.py
Test: All parameter types, value validation, type conversion
```

**Task 16: Create Enhanced Table Handler**
```
Create: tableau_looker_lib/handlers/table_handler.py

Requirements:
- TableHandler class extending BaseHandler
- Handle basic tables and custom SQL tables
- Schema handling and connection linking
- Primary key detection
- Table relationship hints

Include complete unit tests in: tests/test_table_handler.py
Test: Table extraction, schema mapping, connection linking
```

### **Phase 2.3: Enhanced Generation**

**Task 17: Create Enhanced View Generator**
```
Create: tableau_looker_lib/generators/view_generator.py

Requirements:
- ViewGenerator class for complete view files
- Generate dimensions, measures, calculated fields
- Field ordering and grouping
- Drill-down path creation
- Performance optimization hints

Create enhanced templates:
- view.j2 - Complete view template with all field types
- dimension.j2 - Dimension template partial
- measure.j2 - Measure template partial

Include complete unit tests in: tests/test_view_generator.py
Test: Complex view generation, field relationships, template processing
```

**Task 18: Create JSON Converter**
```
Create: tableau_looker_lib/core/json_converter.py

Requirements:
- JSONConverter class orchestrating handler pipeline
- Integration with plugin registry
- Batch element processing
- Error collection and reporting
- Progress tracking

Include complete unit tests in: tests/test_json_converter.py
Test: Handler orchestration, batch processing, error handling
```

### **Phase 2 Integration Testing**

**Task 19: Create Phase 2 Integration Tests**
```
Create: tests/integration/test_phase2_integration.py

Requirements:
- Test calculated field conversions
- Test custom SQL handling
- Test parameter processing
- Test complete view generation
- Performance benchmarks

Create additional test fixtures:
- sample_calculated_fields.twb - Complex calculated fields
- sample_custom_sql.twb - Custom SQL relations
- sample_parameters.twb - Various parameter types

Test: Formula accuracy, custom SQL processing, parameter handling
```

---

## **Phase 3: Complete Feature Set (Weeks 5-6)**

### **Phase 3.1: Advanced Calculation Handlers**

**Task 20: Create LOD Expression Handler**
```
Create: tableau_looker_lib/handlers/lod_handler.py

Requirements:
- LODHandler class extending BaseHandler
- Support: FIXED, INCLUDE, EXCLUDE expressions
- Convert to window functions or subqueries
- Performance optimization strategies
- Complex nested expression handling

Include complete unit tests in: tests/test_lod_handler.py
Test: All LOD types, nested expressions, performance optimization
```

**Task 21: Create Table Calculation Handler**
```
Create: tableau_looker_lib/handlers/table_calc_handler.py

Requirements:
- TableCalcHandler class extending BaseHandler
- Support: RUNNING_SUM, WINDOW_SUM, RANK, PERCENTILE
- Convert to SQL window functions
- Proper ORDER BY and PARTITION BY generation
- Window frame definition

Include complete unit tests in: tests/test_table_calc_handler.py
Test: All table calculation types, window function syntax
```

**Task 22: Create Data Blending Handler**
```
Create: tableau_looker_lib/handlers/blending_handler.py

Requirements:
- BlendingHandler class extending BaseHandler
- Multi-data source blending support
- Generate LookML joins with proper relationships
- Join condition detection and optimization
- Relationship type mapping

Include complete unit tests in: tests/test_blending_handler.py
Test: Complex blending scenarios, join accuracy, relationship types
```

### **Phase 3.2: Worksheet & Dashboard Handlers**

**Task 23: Create Worksheet Handler**
```
Create: tableau_looker_lib/handlers/worksheet_handler.py

Requirements:
- WorksheetHandler class extending BaseHandler
- Support all visualization types
- Extract chart configuration and field usage
- Generate LookML explores
- Filter integration and field role detection

Include complete unit tests in: tests/test_worksheet_handler.py
Test: All visualization types, field mapping, explore generation
```

**Task 24: Create Dashboard Handler**
```
Create: tableau_looker_lib/handlers/dashboard_handler.py

Requirements:
- DashboardHandler class extending BaseHandler
- Handle dashboard layouts and interactivity
- Generate LookML dashboard files
- Action mapping and parameter integration
- Responsive design considerations

Include complete unit tests in: tests/test_dashboard_handler.py
Test: Complex dashboards, interactive elements, layout accuracy
```

**Task 25: Create Filter Handler**
```
Create: tableau_looker_lib/handlers/filter_handler.py

Requirements:
- FilterHandler class extending BaseHandler
- Support all filter types (categorical, quantitative, date)
- Generate proper sql_where conditions
- Multi-select and complex filter logic
- Date range handling

Include complete unit tests in: tests/test_filter_handler.py
Test: All filter types, complex logic, date handling
```

### **Phase 3.3: Complete Generation Suite**

**Task 26: Create Complete Generator Suite**
```
Create: tableau_looker_lib/generators/explore_generator.py
Create: tableau_looker_lib/generators/dashboard_generator.py
Create: tableau_looker_lib/generators/model_generator.py

Requirements:
- Complete LookML file generation
- Proper file organization and dependencies
- Template system with inheritance
- Performance optimization
- Security integration

Create complete template set:
- explore.j2 - Explore template with joins
- dashboard.j2 - Dashboard template with elements
- model.j2 - Model template with explores

Include complete unit tests for each generator
Test: File generation, template processing, dependencies
```

**Task 27: Create File Manager**
```
Create: tableau_looker_lib/generators/file_manager.py

Requirements:
- FileManager class for output organization
- Proper LookML project structure
- File conflict resolution
- Version control integration
- Naming convention enforcement

Include complete unit tests in: tests/test_file_manager.py
Test: File organization, conflict resolution, naming consistency
```

### **Phase 3 Integration Testing**

**Task 28: Create Phase 3 Integration Tests**
```
Create: tests/integration/test_phase3_integration.py

Requirements:
- Test complete feature set
- Test complex workbook processing
- Test dashboard and worksheet conversion
- Performance benchmarks with large files
- Memory usage optimization

Create comprehensive test fixtures:
- sample_complete_workbook.twb - All features
- sample_dashboard.twb - Complex dashboard
- sample_large_workbook.twb - Performance testing

Test: Complete feature coverage, performance, memory usage
```

---

## **Phase 4: ML Integration (Weeks 7-8)**

### **Phase 4.1: ML Components**

**Task 29: Create ML Infrastructure**
```
Create: tableau_looker_lib/ml/ml_engine.py
Create: tableau_looker_lib/ml/base_ml_component.py

Requirements:
- ML engine for model orchestration
- Base ML component class
- Model loading and inference pipeline
- Caching and performance optimization
- Integration with existing handlers

Include complete unit tests in: tests/test_ml_engine.py
Test: Model loading, inference pipeline, performance
```

**Task 30: Create Semantic Field Mapper**
```
Create: tableau_looker_lib/ml/semantic_mapper.py

Requirements:
- SemanticMapper class for field similarity
- Word embeddings and fuzzy matching
- Business term standardization
- Confidence scoring for matches
- Integration with dimension handler

Include complete unit tests in: tests/test_semantic_mapper.py
Test: Field matching accuracy, similarity scoring, business terms
```

**Task 31: Create Formula Classifier**
```
Create: tableau_looker_lib/ml/formula_classifier.py

Requirements:
- FormulaClassifier for complexity analysis
- ML-based classification (Simple/Medium/Complex)
- Conversion strategy suggestions
- Integration with calculated field handler
- Model training infrastructure

Include complete unit tests in: tests/test_formula_classifier.py
Test: Classification accuracy, strategy selection, model performance
```

### **Phase 4.2: Enhanced Handlers with ML**

**Task 32: Enhance Existing Handlers with ML**
```
Update: All existing handlers to integrate ML components
- Add ML-enhanced confidence scoring
- Integrate semantic mapping where applicable
- Add intelligent routing based on ML predictions
- Maintain backward compatibility

Requirements:
- Seamless ML integration
- Fallback to rule-based when ML unavailable
- Performance optimization
- A/B testing capabilities

Update all existing unit tests to cover ML integration
Test: ML enhancement, fallback behavior, performance impact
```

**Task 33: Create Confidence Aggregator**
```
Create: tableau_looker_lib/ml/confidence_aggregator.py

Requirements:
- ConfidenceAggregator class for score combination
- Rule-based + ML confidence fusion
- Uncertainty quantification
- Threshold-based routing to manual review
- Calibration and validation

Include complete unit tests in: tests/test_confidence_aggregator.py
Test: Score combination, threshold optimization, calibration
```

### **Phase 4 Integration Testing**

**Task 34: Create Phase 4 Integration Tests**
```
Create: tests/integration/test_phase4_integration.py

Requirements:
- Test ML-enhanced processing
- Test confidence aggregation
- Test intelligent routing
- Performance benchmarks with ML
- A/B testing framework

Test: ML accuracy, performance impact, intelligent routing
```

---

## **Phase 5: Production Ready (Weeks 9-10)**

### **Phase 5.1: Enterprise Features**

**Task 35: Create Batch Processing System**
```
Create: tableau_looker_lib/batch/batch_processor.py
Create: tableau_looker_lib/batch/migration_tracker.py

Requirements:
- BatchProcessor for multi-file processing
- MigrationTracker for progress and state management
- Parallel processing with resource management
- Failure recovery and resume capability
- Progress reporting and ETA calculation

Include complete unit tests in: tests/test_batch_processor.py
Test: Batch processing, failure recovery, progress tracking
```

**Task 36: Create Enterprise Validation**
```
Create: tableau_looker_lib/validation/enterprise_validator.py

Requirements:
- EnterpriseValidator for comprehensive validation
- Data quality checks and performance validation
- Compliance checking (SOX, governance)
- Detailed validation reports
- Remediation suggestions

Include complete unit tests in: tests/test_enterprise_validator.py
Test: All validation types, compliance checking, report generation
```

### **Phase 5.2: Monitoring & Reporting**

**Task 37: Create Monitoring System**
```
Create: tableau_looker_lib/monitoring/performance_monitor.py
Create: tableau_looker_lib/monitoring/audit_logger.py

Requirements:
- PerformanceMonitor for system health
- AuditLogger for compliance trail
- Metrics collection and alerting
- Integration with monitoring systems
- Secure log storage

Include complete unit tests in: tests/test_monitoring.py
Test: Metrics accuracy, alerting, log security
```

**Task 38: Create Reporting System**
```
Create: tableau_looker_lib/reporting/migration_reporter.py

Requirements:
- MigrationReporter for comprehensive reports
- HTML/PDF report generation
- Executive summaries and technical details
- Customizable report templates
- Performance analytics

Include complete unit tests in: tests/test_reporting.py
Test: Report generation, customization, analytics
```

### **Phase 5.3: Production Deployment**

**Task 39: Create Production Infrastructure**
```
Create: tableau_looker_lib/deployment/deployment_manager.py
Create: tableau_looker_lib/config/config_manager.py

Requirements:
- DeploymentManager for automated deployment
- ConfigManager for environment management
- CI/CD integration and automated testing
- Security and credential management
- Health checking and monitoring

Include complete unit tests in: tests/test_deployment.py
Test: Deployment automation, configuration management, security
```

**Task 40: Create Final Integration and Performance Tests**
```
Create: tests/integration/test_production_ready.py
Create: tests/performance/test_enterprise_scale.py

Requirements:
- Complete end-to-end testing
- Enterprise-scale performance testing
- Load testing with 1000+ files
- Memory usage and optimization
- Security and compliance validation

Test: Enterprise scale, performance, security, compliance
```

---

## **General Coding Guidelines for Claude**

### **Code Quality Standards:**
- **Type hints** for all functions and methods
- **Docstrings** for all classes and methods (Google style)
- **Error handling** with specific exceptions
- **Logging** at appropriate levels
- **Configuration** externalized, not hardcoded
- **Security** considerations for file handling and SQL

### **Testing Requirements:**
- **Unit tests** with 80%+ coverage for each module
- **Integration tests** for component interactions
- **Performance tests** for large files
- **Security tests** for input validation
- **Mock external dependencies** appropriately

### **Documentation:**
- **API documentation** for all public methods
- **Usage examples** for complex features
- **Configuration guides** for different environments
- **Troubleshooting guides** for common issues

### **Performance Considerations:**
- **Memory efficient** processing for large files
- **Streaming** where possible for XML processing
- **Caching** for repeated operations
- **Parallel processing** for batch operations
- **Profiling** and optimization for bottlenecks

### **Security Requirements:**
- **Input validation** for all user inputs
- **SQL injection** prevention
- **Path traversal** prevention for file operations
- **Credential management** for database connections
- **Audit logging** for security events

Each task should be completed with full implementation, comprehensive tests, and proper documentation before moving to the next task.
