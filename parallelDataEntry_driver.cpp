#include <iostream>
#include <string>
#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <assert.h>
#include <omp.h>
#include <time.h>
#include "timer.c"
// Use on-disk DB option from sqlite3 for parallel data-entry
// Standard libraries to communicate with sqlite3 interface are used
// comparison performed
//

// Pointer to the sqlite3 databse handle
//
sqlite3 *db;

// Global Vars
//
int MAX_THREADS = 0;
int NUM_OF_QUERIES = 0;

// Run experiments on 6 tables
//
int NUM_OF_TABLES = 6;
FILE *fp;
int count = 1;

// Call-back function from sql query
//
static int callback(void *NotUsed, int argc, char **argv, char **azColName)
{
	int i;
	for(i=0; i<argc; i++){
   	printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
   	}
   	printf("\n");
   	return 0;
}

// Initalize NUM_OF_TABLES tables in the databse
//
void initTables()
{
	for(int i = 0; i < NUM_OF_TABLES; i++)
	{
		char sql[60];
		char *zErrMsg = 0;
		sprintf(sql, "CREATE TABLE T%d(ID INT, ATTR1 INT, ATTR2 INT);", i);
		// Execute query
		//
		int rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
		if(rc != SQLITE_OK)
		{
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}
		else 
		{
			printf("Table [%d] created successfully\n", i);
		}
	}
}

// Run all queries on one Table
//
void runQueriesTables1()
{
	int i;
		
	for(int j = 0; j< NUM_OF_QUERIES*NUM_OF_TABLES; j++)
	{
		char sql[60];
		char *zErrMsg = 0;
		// Create INSERT Query
		//
		sprintf(sql, "INSERT INTO T%d VALUES(100, 50, 5)", i);
		int rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
		if(rc != SQLITE_OK)
		{
			fprintf(stdout, "Insert Error at : %s\n", zErrMsg);
		}
	}
	
}

// Run Queries divided equally among the numTables
//
void runQueriesTables(int numTables)
{
	int i;

	#pragma omp parallel for private(i)
	for(i = 0; i < numTables; i++)
	{
		for(int j = 0; j < NUM_OF_QUERIES*NUM_OF_TABLES/(numTables*(i+1)); j++)
		{
			char sql[60];
			char *zErrMsg = 0;
			sprintf(sql, "INSERT INTO T%d VALUES(150, 50, 15)", i);
			int rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
			if(rc != SQLITE_OK)
			{
				fprintf(stderr, "Insert error : %s\n", zErrMsg);
			}

		}
	}
}

void runDriver()
{
	printf("\nRUN CONFIGURATION:\n=== Number of INSERT queries/thread = [%d]\nTotal INSERT queries = [%d];\n=== MAX_THREADS [%d]\n\n", NUM_OF_QUERIES
	, NUM_OF_TABLES * NUM_OF_QUERIES,MAX_THREADS);

	initTables();

	// initializing timer
	//
	stopwatch_init();
	struct stopwatch_t *timer = stopwatch_create();
	assert(timer);

	// Run queries on single table
	//
	printf("\n===> STARTING: Sequential Module to Insert %d queries in NUM_OF_TABLES = 1 \n", NUM_OF_QUERIES*NUM_OF_TABLES);
	stopwatch_start(timer);
	runQueriesTables1();
	long double timeTables1 = stopwatch_stop(timer);
	printf("\nTIME: [%Lg] secs\n", timeTables1);
	//fprintf(fp, "%d %Lg\n",count, timeTables1);
	count+= 1;

	// Queries divided among multiple tables
	//
	for(int i = 1; i<=NUM_OF_TABLES; i++)
	{
		printf("\n===> STARTING: Parallel Module to Insert %d queries divided among NUM_OF_TABLES = %d \n", NUM_OF_QUERIES*NUM_OF_TABLES, i);
		stopwatch_start(timer);
		runQueriesTables(i);
		long double timeTables = stopwatch_stop(timer);
		printf("\nTIME: [%Lg] secs\n", timeTables);
		fprintf(fp, "%d %Lg\n",count, timeTables);
		count += 1;
	}

}

int main(int argc, char* argv[])
{
	// Opening handle to sqlite3 test database
	//
	if (argc < 2 || argc > 3)
	{
		printf("Usage: ./parallelDriver [# of Queries/Thread] [Optional: Max number of threads]");
		return 0;
	}
	
	fp = fopen("plot.txt", "w");
	//system("lscpu");
	printf("\n");
	// Initializing global params
	//
	NUM_OF_QUERIES = atoi(argv[1]);
	if(argc == 3) MAX_THREADS = atoi(argv[2]);
	else 
	{
		if(omp_get_max_threads() < 16) MAX_THREADS = omp_get_max_threads();
		else MAX_THREADS = 16;
	}
	// Create a in-memory databse
	//
	int result = sqlite3_open("test.db", &db);
	if(result != SQLITE_OK)
	{
		printf("\n>> Error loading handle to Database");
	}
	else
	{
		printf("\n===== Optimized Parallel Data Entry Driver ======\n ");
	}

	runDriver();
	sqlite3_close(db);
	fclose(fp);
	//Remove test.db to create a fresh instance the next time
	//
	int rval = system("rm test.db");
	rval = system("gnuplot plot.p");
	rval = system ("evince result.ps");
	printf("\n\n");
	// Test code for in-memory database
	//
	/*printf(">>Now using In-memory database just for Comparison\n");
	sqlite3_open(":memory:", &db);
	runDriver();
	sqlite3_close(db);
	printf("\n\n");
	*/
	return 0;
}
