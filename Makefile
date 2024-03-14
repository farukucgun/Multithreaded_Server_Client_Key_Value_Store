all: clientk serverk

clientk: clientk.o
	gcc -Wall -lpthread -o $@ $^ 

serverk: serverk.o
	gcc -Wall -lpthread -o $@ $^

%.o: %.c
	gcc -Wall -lpthread -c $< -o $@

clean:
	rm -f clientk.o clientk
	rm -f serverk.o serverk
	rm -f dataset*
	ipcrm --all=msg