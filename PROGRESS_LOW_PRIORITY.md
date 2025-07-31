# Low Priority Features - Future Enhancements

## Overview
These are infrastructure and polish features from Phase 1 & 2 that enhance the system but are not blocking core migration functionality.

---

## Phase 1 Low Priority Items

### **Task 11: Integration Test Suite** ⏳ PENDING
**Purpose**: End-to-end pipeline testing
- Complete XML → JSON → LookML pipeline testing
- Sample Tableau files with expected outputs
- Performance benchmarks
- **Priority**: Infrastructure enhancement - current unit tests cover core functionality

---

## Phase 2 Low Priority Items

### **Task 16: Enhanced Table Handler** ⏳ PENDING
**Purpose**: Advanced table processing features
- Schema mapping and connection linking
- Primary key detection
- Table relationship hints
- **Priority**: Enhancement - basic table handling already works

### **Task 17: Enhanced View Generator** ⏳ PENDING
**Purpose**: View file polish and organization
- Field ordering and logical grouping
- Drill-down path creation
- Performance optimization hints
- **Priority**: UI/UX enhancement - basic view generation works

### **Task 18: JSON Converter** ⏳ PENDING
**Purpose**: Pipeline orchestration refactoring
- Handler pipeline optimization
- Batch element processing
- Enhanced error collection
- **Priority**: Architectural improvement - current pipeline works

### **Task 19: Phase 2 Integration Tests** ⏳ PENDING
**Purpose**: Comprehensive testing framework
- Calculated field conversion testing
- Parameter processing tests
- Performance benchmarks
- **Priority**: Testing infrastructure - unit tests cover functionality

---

## Rationale for Low Priority

These items improve **system quality and maintainability** but don't block **core migration functionality**:

- ✅ **Core migration works** without these enhancements
- ✅ **Business value delivered** through existing calculated fields system
- ✅ **Production ready** for basic-to-intermediate Tableau workbooks
- 🔄 **Future enhancements** when system scales or needs polish

---

## Implementation Order (When Prioritized)

1. **Enhanced View Generator** (Task 17) - Most user-visible improvements
2. **Enhanced Table Handler** (Task 16) - Database integration improvements
3. **JSON Converter** (Task 18) - Performance and architecture
4. **Integration Test Suite** (Task 11) - Quality assurance
5. **Phase 2 Integration Tests** (Task 19) - Comprehensive validation

---

*Status: ⏳ DEFERRED*
*Rationale: Focus on high-impact features (custom SQL, date functions, configuration)*
