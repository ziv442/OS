# Variables
CXX := g++
AR := ar
DEBUG ?= 1
CXXFLAGS := -Wall -Wextra -g -std=c++20

LIB := libMapReduceFramework.a
OBJS := $(patsubst %.cpp, %.o, $(wildcard *.cpp))
HEADERS := $(filter-out MapReduceClient.h, MapReduceFramework.h, $(wildcard *.h))
TAR_NAME := ex3.tar

# Default rule
all: $(LIB)
	rm $(OBJS)

# Static library rule
$(LIB): $(OBJS)
	$(AR) rcs $@ $^

# Object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Tarball
tar:
	tar --exclude=MapReduceClient.h --exclude=MapReduceFramework.h -cvf $(TAR_NAME) *.cpp *.h Makefile README

# Clean rule
clean:
	rm -f *.o $(LIB) $(TAR_NAME)

.PHONY: all clean tar