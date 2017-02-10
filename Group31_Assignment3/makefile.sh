CC = gcc
CFLAGS = -Wall
DEPS = dberror.h storage_mgr.h dt.h buffer_mgr_stat.h buffer_mgr.h test_helper.h record_mgr.h expr.h tables.h 
OBJ1 = dberror.o record_mgr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o test_assign3_1.o rm_serializer.o expr.o
OBJ2 = dberror.o record_mgr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o rm_serializer.o expr.o test_expr.o


%.o: %.c $(DEPS)
		$(CC) $(CFLAGS) -c -o $@ $<

all: assign3 expr
		
assign3: $(OBJ1)
		gcc $(CFLAGS) -o $@ $^

expr: $(OBJ2)
		gcc $(CFLAGS) -o $@ $^
		
clean:
		rm *.o
		rm assign3 expr makefile
