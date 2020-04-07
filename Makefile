#### PROJECT SETTINGS ####
# Compiler used
CXX ?= g++
# Extension of source files used in the project
SRC_EXT = cpp
# Path to the source directory, relative to the makefile
SRC_PATH = ./src
# General compiler flags
COMPILE_FLAGS = -std=c++17 -Wall -Werror -D__FAAS_SRC -fdata-sections -ffunction-sections
# Additional release-specific flags
RCOMPILE_FLAGS = -DNDEBUG -O3
# Additional debug-specific flags
DCOMPILE_FLAGS = -DDEBUG -g
# Add additional include paths
INCLUDES = -I$(SRC_PATH) -I./include -I./deps/out/include \
	-DFMT_HEADER_ONLY -I./deps/fmt/include \
	-I./deps/GSL/include \
	-I./deps/json/single_include
# General linker settings
ABSL_LIBRARIES = $(shell find deps/out/lib/libabsl_*.a -printf '%f\n' \
                   | sed -e 's/libabsl_\([a-z0-9_]\+\)\.a/-labsl_\1/g')
LINK_FLAGS = -Ldeps/out/lib \
    -Wl,-Bstatic -luv_a -lhttp_parser -lnghttp2_static \
    -Wl,--start-group $(ABSL_LIBRARIES) -Wl,--end-group \
    -Wl,-Bdynamic -lpthread -ldl -Wl,--gc-sections
# Additional release-specific linker settings
RLINK_FLAGS =
# Additional debug-specific linker settings
DLINK_FLAGS =
#### END PROJECT SETTINGS ####

# These options can be overridden in config.mk
ENABLE_PROFILING = 0

ifneq ("$(wildcard config.mk)","")
    include config.mk
endif

ifeq ($(CXX),clang++)
    COMPILE_FLAGS += -Wthread-safety -Wno-unused-private-field
endif

ifeq ($(ENABLE_PROFILING),1)
    COMPILE_FLAGS += -D__FAAS_ENABLE_PROFILING
endif

# Function used to check variables. Use on the command line:
# make print-VARNAME
# Useful for debugging and adding features
print-%: ; @echo $*=$($*)

# Shell used in this makefile
# bash is used for 'echo -en'
SHELL = /bin/bash
# Clear built-in rules
.SUFFIXES:

# Verbose option, to output compile and link commands
export V := 0
export CMD_PREFIX := @
ifeq ($(V),1)
    CMD_PREFIX :=
endif

# Combine compiler and linker flags
release: export CXXFLAGS := $(CXXFLAGS) $(COMPILE_FLAGS) $(RCOMPILE_FLAGS)
release: export LDFLAGS := $(LDFLAGS) $(LINK_FLAGS) $(RLINK_FLAGS)
debug: export CXXFLAGS := $(CXXFLAGS) $(COMPILE_FLAGS) $(DCOMPILE_FLAGS)
debug: export LDFLAGS := $(LDFLAGS) $(LINK_FLAGS) $(DLINK_FLAGS)

# Build and output paths
release: export BUILD_PATH := build/release
release: export BIN_PATH := bin/release
debug: export BUILD_PATH := build/debug
debug: export BIN_PATH := bin/debug

# Find all source files in the source directory, sorted by most
# recently modified
SOURCES = $(shell find $(SRC_PATH) -name '*.$(SRC_EXT)' -printf '%T@\t%p\n' \
            | sort -k 1nr | cut -f2-)
BIN_SOURCES = $(shell find $(SRC_PATH)/bin -name '*.$(SRC_EXT)' -printf '%T@\t%p\n' \
                | sort -k 1nr | cut -f2-)

# Set the object file names, with the source directory stripped
# from the path, and the build path prepended in its place
OBJECTS = $(SOURCES:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
# Set the dependency files that will be used to add header dependencies
DEPS = $(OBJECTS:.o=.d)

BIN_OBJECTS = $(BIN_SOURCES:$(SRC_PATH)/%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
NON_BIN_OBJECTS = $(filter-out $(BIN_OBJECTS),$(OBJECTS))

BIN_NAMES = $(BIN_OBJECTS:$(BUILD_PATH)/%.o=%)
BIN_OUTPUTS = $(BIN_OBJECTS:$(BUILD_PATH)/bin/%.o=$(BIN_PATH)/%)

TIME_FILE = $(dir $@).$(notdir $@)_time
START_TIME = date '+%s' > $(TIME_FILE)
END_TIME = read st < $(TIME_FILE) ; \
	$(RM) $(TIME_FILE) ; \
	st=$$((`date '+%s'` - $$st - 86400)) ; \
	echo `date -u -d @$$st '+%H:%M:%S'`

# Standard, non-optimized release build
.PHONY: release
release: dirs
	@echo "Beginning release build"
	@$(START_TIME)
	@$(MAKE) all --no-print-directory
	@echo -n "Total build time: "
	@$(END_TIME)

# Debug build for gdb debugging
.PHONY: debug
debug: dirs
	@echo "Beginning debug build"
	@$(START_TIME)
	@$(MAKE) all --no-print-directory
	@echo -n "Total build time: "
	@$(END_TIME)

# Create the directories used in the build
.PHONY: dirs
dirs:
	@mkdir -p $(dir $(OBJECTS))
	@mkdir -p $(BIN_PATH)

# Removes all build files
.PHONY: clean
clean:
	@echo "Deleting directories"
	@$(RM) -r build
	@$(RM) -r bin

# Main rule, checks the executable and symlinks to the output
all: $(BIN_OUTPUTS)

# Link the executable
$(BIN_PATH)/%: $(BUILD_PATH)/bin/%.o $(NON_BIN_OBJECTS)
	@echo "Linking: $@"
	$(CMD_PREFIX)$(CXX) $(NON_BIN_OBJECTS) $< $(LDFLAGS) -o $@

.SECONDARY: $(OBJECTS)

# Add dependency files, if they exist
-include $(DEPS)

# Source file rules
# After the first compilation they will be joined with the rules from the
# dependency files to provide header dependencies
$(BUILD_PATH)/%.o: $(SRC_PATH)/%.$(SRC_EXT)
	@echo "Compiling: $< -> $@"
	$(CMD_PREFIX)$(CXX) $(CXXFLAGS) $(INCLUDES) -MP -MMD -c $< -o $@
