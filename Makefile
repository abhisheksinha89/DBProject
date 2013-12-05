all:
	g++ -O2 parallelDataEntry_driver.cpp -l sqlite3 -o parallelDriver -lm -fopenmp
	g++ -O2 parallelDataEntry_random.cpp -l sqlite3 -o parallelRandom -lm -fopenmp	
clean:
	rm parallelDriver
	rm parallelRandom
