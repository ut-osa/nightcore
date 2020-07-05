#### PROJECT SETTINGS ####
# Compiler used
CXX ?= g++
# Extension of source files used in the project
SRC_EXT = cpp
# General compiler flags
COMPILE_FLAGS = -std=c++17 -Wall -Werror -D__FAAS_PYTHON_BINDING -fPIC \
	-DNDEBUG -O3 -DDCHECK_ALWAYS_ON
# Add additional include paths
INCLUDES = -I./src \
	-DFMT_HEADER_ONLY -I./deps/fmt/include \
	-I./deps/GSL/include \
	-I./deps/json/single_include \
	-I./deps/pybind11/include \
	$(shell python3-config --includes)
# General linker settings
LINK_FLAGS =
#### END PROJECT SETTINGS ####

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

BUILD_PATH := build
LIBRARY_OUTPUT_PATH := faas
PY_EXT_SUFFIX = $(shell python3-config --extension-suffix)
LIBRARY := $(LIBRARY_OUTPUT_PATH)/_faas_native$(PY_EXT_SUFFIX)

CXXFLAGS := $(CXXFLAGS) $(COMPILE_FLAGS)
LDFLAGS := $(LDFLAGS) $(LINK_FLAGS)

SOURCES = module.cpp \
	src/base/logging.cpp \
	src/common/func_config.cpp \
	src/ipc/base.cpp \
	src/ipc/fifo.cpp \
	src/ipc/shm_region.cpp \
	src/utils/fs.cpp \
	src/utils/socket.cpp \
	src/worker/worker_lib.cpp \
	src/worker/event_driven_worker.cpp

# Set the object file names, with the source directory stripped
# from the path, and the build path prepended in its place
OBJECTS = $(SOURCES:%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
# Set the dependency files that will be used to add header dependencies
DEPS = $(OBJECTS:.o=.d)

TIME_FILE = $(dir $@).$(notdir $@)_time
START_TIME = date '+%s' > $(TIME_FILE)
END_TIME = read st < $(TIME_FILE) ; \
	$(RM) $(TIME_FILE) ; \
	st=$$((`date '+%s'` - $$st - 86400)) ; \
	echo `date -u -d @$$st '+%H:%M:%S'`

# Standard, non-optimized release build
.PHONY: release
release: dirs
	@echo "Beginning build"
	@$(START_TIME)
	@$(MAKE) all --no-print-directory
	@echo -n "Total build time: "
	@$(END_TIME)

# Create the directories used in the build
.PHONY: dirs
dirs:
	@mkdir -p $(dir $(OBJECTS))

# Removes all build files
.PHONY: clean
clean:
	@echo "Deleting directories"
	@$(RM) -r build
	@$(RM) -r $(LIBRARY_OUTPUT_PATH)/*.so

# Main rule, checks the executable and symlinks to the output
all: $(LIBRARY)

# Link the executable
$(LIBRARY): $(OBJECTS)
	@echo "Linking: $@"
	$(CMD_PREFIX)$(CXX) $(OBJECTS) $(LDFLAGS) -shared -o $@

.SECONDARY: $(OBJECTS)

# Add dependency files, if they exist
-include $(DEPS)

# Source file rules
# After the first compilation they will be joined with the rules from the
# dependency files to provide header dependencies
$(BUILD_PATH)/%.o: %.$(SRC_EXT)
	@echo "Compiling: $< -> $@"
	$(CMD_PREFIX)$(CXX) $(CXXFLAGS) $(INCLUDES) -MP -MMD -c $< -o $@
