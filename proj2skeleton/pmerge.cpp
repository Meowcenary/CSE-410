/** Implementation of parallel merge sort using hierarchical thread */

#include <iostream>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <iomanip>
#include "record.h"
#include<mutex>
#include<cmath>
using namespace std;

// the mutex and condition variable are used for synchronization between threads
pthread_mutex_t mut=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t con=PTHREAD_COND_INITIALIZER;

// the shared variable that denotes the state of the system
int shared_status = STATUS_INACTIVE;

// name of the attributes
string attributeList[] = {"id", "geneder","firstname", "middlename", "lastname", "cityname", "statename", "ssn"};


/*
function name - compareRecords 
task - compares employee at index1 with employee at index2 based on attributes in v 
arguments - 
	index1 - index in the dataset 
	index2 - index in the dataset
	v      - sorting attributes	
output -
	1 - if employee of index1 is greater than employee of index2 
	0 - otherwise
*/	
int compareRecords( int index1, int index2, vector<int> &v)
{
  /* fill in code here to compare records, make sure to compare the records on attributes in order */
	/*
	from notes:	sortingattributes one or more numbers representing indicies of sorting criteria
			1 for gender, 2 for first name, 3 for middle name, 4 for last name, 5 for city name
			6 for state name, 7 for social security number
	*/

	/*
		How should records be compared when more than one attribute is being used for the comparison?

	*/
	for(int i = 0; i < v.size(); i++)
	{
		// sort on gender
		if(v[i] == 1)
		{
			if(dataSet[index1]->gender > dataSet[index2]->gender)	
			{
				return 1;
			}
			else if(dataSet[index1]->gender < dataSet[index2]->gender)	
			{
				return 0;
			}
		}
		// sort on first name, 2
		else if(v[i] == 2)
		{
			if(dataSet[index1]->firstName > dataSet[index2]->firstName)	
			{
				return 1;
			}
			else if(dataSet[index1]->firstName < dataSet[index2]->firstName)	
			{
				return 0;
			}
		}
		else if(v[i] == 3)
		{
			if(dataSet[index1]->middleName > dataSet[index2]->middleName)	
			{
				return 1;
			}
			else if(dataSet[index1]->middleName < dataSet[index2]->middleName)	
			{
				return 0;
			}
			
		}
		else if(v[i] == 4)
		{
			if(dataSet[index1]->lastName > dataSet[index2]->lastName)	
			{
				return 1;
			}
			else if(dataSet[index1]->lastName < dataSet[index2]->lastName)	
			{
				return 0;
			}
		}
		else if(v[i] == 5)
		{
			if(dataSet[index1]->cityName > dataSet[index2]->cityName)	
			{
				return 1;
			}
			else if(dataSet[index1]->cityName < dataSet[index2]->cityName)	
			{
				return 0;
			}
		}
		else if(v[i] == 6)
		{	
			if(dataSet[index1]->stateName > dataSet[index2]->stateName)	
			{
				return 1;
			}
			else if(dataSet[index1]->stateName < dataSet[index2]->stateName)	
			{
				return 0;
			}
		}
		else if(v[i] == 7)
		{
			if(dataSet[index1]->ssNumber > dataSet[index2]->ssNumber)	
			{
				return 1;
			}
			else if(dataSet[index1]->ssNumber < dataSet[index2]->ssNumber)	
			{
				return 0;
			}
		}
	}
	return 0; // if they are equal in every way return 0
}

/*
function name - swap
task - swaps two pointers 
arguments -
	index1, index2
output - none
*/
void swap(int index1, int index2)
{
	employeeRecord* t 	= dataSet[index1];
	dataSet[index1]	  	= dataSet[index2];
	dataSet[index2]        	= t;	

}


/*
function name - getTimeUsed
task - calculates the time difference between time a and time b
arguments -
	startTime, endTime - time structure, startTime <= endTime
output - time difference in microsecs between startTime and endTime
*/
double getTimeUsed(struct timeval &startTime, struct timeval &endTime)
{
	return (endTime.tv_sec - startTime.tv_sec)*1000000 + (endTime.tv_usec - startTime.tv_usec);
}


/*
function name - writeData
task - entry function for writerThread, waits for sorting to be finished and then writes the sorted list into the output file
arguments -
	output file name is passed as argument, return the cpu time used in the thread
output - none
*/
void* writeData(void * arg)
{
	// wait for the sorting to finish
	pthread_mutex_lock(&mut);
	while(shared_status < STATUS_SORTING_COMPLETED)
	{
		cout << "Waiting for Others" << endl;
		pthread_cond_wait(&con, &mut);
		
	}
	cout << "Entered write" << endl;
	if(shared_status != STATUS_ABORT)
		shared_status = STATUS_WRITING;
	pthread_mutex_unlock(&mut);

	// exit the thread if abort status is set
	if(shared_status == STATUS_ABORT)
		pthread_exit(NULL);

	// read the argument

	argListforRW* aList = static_cast<argListforRW*> (arg);	
        string fileName = aList->fileName;
	int starting = 0;
	int ending   = dataSet.size()-1;
	retVal* rVal = aList->rVal;
	struct timeval now, later;
	gettimeofday(&now, NULL);

	// open the file to write
	ofstream ofile;
	ofile.open(fileName.c_str());
	if(ofile.is_open()){
		stringstream ss;
		for (int i = starting; i <= ending; i++){
			ss   << setprecision(3) << dataSet[i]->eid         << setw(10) << dataSet[i]->gender 
		     	<< setw(20)        << dataSet[i]->firstName 
		     	<< setw(5)         << dataSet[i]->middleName  << setw(20) << dataSet[i]->lastName  
		     	<< setw(20)        << dataSet[i]->cityName    << setw(20) << dataSet[i]->stateName 	
		     	<< setw(20)        << dataSet[i]->ssNumber  << endl;
		}

		ofile << ss.rdbuf();
		ofile.close();
	}
	else{
		cout << "Failed to open the output file " << fileName << endl;
		pthread_exit(NULL);
	}

	// change the system state to write_completed
	pthread_mutex_lock(&mut);
	shared_status = STATUS_WRITING_COMPLETED;
	pthread_mutex_unlock(&mut);

	gettimeofday(&later, NULL);
	rVal->timeUsed = getTimeUsed(now, later);

	pthread_exit(NULL);
}



/*
function name - insertion_sort
task - sorts the employee between start index and end index and returns the frequency of the keyword within that range
arguments -
	startIndex, endIndex
	keyword - search keyword
	v - sorting attributes
output - returns number of times the keyword is found in the records between startIndex and endIndex
*/
int insertion_sort(int startIndex, int endIndex, string keyword, vector<int>& v)
{
  // if statrIndex > endIndex return 0
  // otherwise, write code to simultaneously search the keyword and sort the records between startIndex and endIndex using insertion sort	
	int j;	
	int p;
	for(p = startIndex; p < endIndex+1 ; p++)
	{
		j = p;
		while(j > 0 && compareRecords(j-1, j, v))
		{
			swap(j-1,j);
			j--;
		}
	}	
	
	int keywordCount = 0;
	for(int i = startIndex; i <= endIndex; i++)
	{
		if(dataSet[i]->stateName == keyword)
			keywordCount++;
	}
	return keywordCount;		
}

/*
function name - merge
task - merges two sorted arrays at index i and at index j into one sorted list, i < j
arguments -
	i, mid  - start and end index of the first sorted array in the dataset
	bi,  j  - start and end index of the second sorted array in the dataset 
	v - sorting attributes
output - none
*/ 
void merge(int i, int mid, int bi, int j, vector <int>& v)
{
    
        int ai = i;
    	vector<employeeRecord*> tempList;

        while(ai <= mid && bi <= j) {
                if (compareRecords(ai, bi, v))
                        tempList.push_back(dataSet[bi++]);
                else                    
                        tempList.push_back(dataSet[ai++]);
        }

        while(ai <= mid) {
                tempList.push_back(dataSet[ai++]);
        }

        while(bi <= j) {
                tempList.push_back(dataSet[bi++]);
        }

        for (ai = 0; ai < (j-i+1) ; ai++){
                dataSet[i+ai] = tempList[ai];
	}

}

/*
function name - mergesort
task - entry function for sorterThread and all child threads
arguments -
	arg - a pointer to the argument structure
output - none
*/
void * mergesort(void *arg)
{
	struct timeval now, later;
	gettimeofday(&now, NULL);
	argList* aList = static_cast<argList *>(arg);
	retVal *rVal = aList->rVal;
	int startIndex = aList->startIndex;
	int endIndex = aList->endIndex;
	string keyword = aList->keyword;
	int threadno = aList->threadno;
	int minSize = aList->minSize;
	vector<int> criteria = aList->criteria;
	
	// wait for reader to finish and then change the system state, if the abort status is set, exit the thread 
	// wait for the reading to finish
	pthread_mutex_lock(&mut);
	while(shared_status < STATUS_READING_COMPLETED)
	{
		cout << "Waiting for Reading" << endl;
		pthread_cond_wait(&con, &mut);
	}
	if(shared_status != STATUS_ABORT)
		shared_status = STATUS_SORTING;
	else{}
	pthread_mutex_unlock(&mut);

	// exit the thread if abort status is set
	if(shared_status == STATUS_ABORT)
		pthread_exit(NULL);
	else{}
	if(threadno == 0)
		endIndex = dataSet.size()-1;
	
	// if the number of records to be sorted is below the threshold, simply call insertion sort
	cout << "Start : End || " << startIndex << " : " << endIndex << endl; 
	if(endIndex - startIndex <= minSize)
	{
		rVal->frequency = insertion_sort(startIndex, endIndex, keyword, criteria);
	}
	else
	{
		cout << "Tested condition: " << endIndex - startIndex << " vs minSize: " << minSize << endl;
		
		pthread_t child1, child2;
		retVal *rVal1, *rVal2;
		int mid = floor((endIndex-startIndex)/2);
		cout << "Start Index: " << startIndex << ", Midpoint: " << startIndex+mid << ", End Index: " << endIndex <<  endl;
		
		rVal1 = new retVal();
		argList* child1Arg = new argList();
		child1Arg->startIndex = startIndex;
		child1Arg->endIndex = startIndex+mid;
		child1Arg->keyword = keyword;
		child1Arg->threadno = threadno+1;
		child1Arg->minSize = minSize;
		child1Arg->criteria = criteria;
		child1Arg->rVal = rVal1;
		
		rVal2 = new retVal();
		argList* child2Arg = new argList();
		child2Arg->startIndex = startIndex+mid+1;
		child2Arg->endIndex = endIndex;
		child2Arg->keyword = keyword;
		child2Arg->threadno = threadno+1;
		child2Arg->minSize = minSize;
		child2Arg->criteria = criteria;
		child2Arg->rVal = rVal2;
		
		int ret1 = pthread_create(&child1, NULL, mergesort, (void *) child1Arg);
		if(ret1)
		{
			cout << "MergeThread " << threadno << "'s Child 1 Thread could not be created." << endl;
			exit(EXIT_FAILURE);
		}
		else{}
		
		int ret2 = pthread_create(&child2, NULL, mergesort, (void *) child2Arg);
		if(ret2)
		{
			cout << "MergeThread " << threadno << "'s Child 2 Thread could not be created." << endl;
			exit(EXIT_FAILURE);
		}
		else{}
		pthread_join(child1, NULL);
		pthread_join(child2, NULL);
		//cout << "Successful join" << endl;
		merge(child1Arg->startIndex, child1Arg->endIndex, child2Arg->startIndex, child2Arg->endIndex, criteria);
		//cout << "Successful merge" << endl;
		rVal->frequency = rVal->frequency + rVal1->frequency + rVal2->frequency;//add child amounts of keyword seen to parent here
		delete rVal1;
		delete rVal2;
	}
		
    // otherwise,
		// 	we need to create one thread and assign it to sort half the records
		// 	set up thread attributes, create the thread, and pass arguments to the child thread
		//	sort other half by calling insertion sort 
					// SMART THING TO DO -----> rVal->frequency = insertion_sort(startIndex, mid, keyword, criteria);
		//	wait for the child thread to finish
		//	merge the results from its child threads

     // update the summary
	gettimeofday(&later, NULL);
	rVal->timeUsed = getTimeUsed(now, later);
		
    // if sorting is finished, notify the writer thread
	if(threadno == 0){
		pthread_mutex_lock(&mut);
		shared_status = STATUS_SORTING_COMPLETED;
		pthread_cond_signal(&con);
		pthread_mutex_unlock(&mut);
	}
	pthread_exit(NULL);
}


/*
function name - readData
task - entry function for readerThread, reads employee records into dataSet and notifies the sorterThread when it is done
arguments -
	arg - a pointer to the argument structure
output - none
*/
void * readData(void *arg)
{
	struct timeval now, later;
	gettimeofday(&now, NULL);
	argListforRW* aList = static_cast<argListforRW *>(arg);
	retVal *rVal = aList->rVal;
	string fileName = aList->fileName;

	// change the system state

	pthread_mutex_lock(&mut);
	shared_status = STATUS_READING;
	pthread_mutex_unlock(&mut);

	// read from the file
	ifstream infile;
        infile.open(fileName.c_str());
	string line;
	if(infile.is_open()){
        	while(getline(infile, line)){
                	stringstream iss(line);
			vector<string> record;
			string str;
			while(getline(iss, str, ',')){
				record.push_back(str);

			}	
			employeeRecord* eR = new employeeRecord(record);
                	dataSet.push_back(eR);
        	}
	        infile.close();
	}
	else{ // set the abort status and notifies all the waiting threads
		cout << "Failed to open the input file " << fileName << endl;
		pthread_mutex_lock(&mut);
		shared_status = STATUS_ABORT;
		pthread_cond_broadcast(&con);
		pthread_mutex_unlock(&mut);
		pthread_exit(NULL);
	}
	// change the system state
	pthread_mutex_lock(&mut);
	shared_status = STATUS_READING_COMPLETED;
	pthread_cond_broadcast(&con);
	pthread_mutex_unlock(&mut);
	
	gettimeofday(&later, NULL);
	
	// update the summary
	rVal->frequency = dataSet.size();		   
	rVal->timeUsed  = getTimeUsed(now, later); 
	pthread_exit(NULL);

}
/*
function name - validateParams
arguments -
	number of user input and pointer to the user inputs,
	output - 0 if error, 1 otherwise
*/
int validateParams(int argc, char** argv, char* inputFile, char* outputFile, string& keyword, int& minSize, vector<int>& criteria)
{
	if(argc < 6){
		strcpy(inputFile, "./input/test.csv");
		keyword = "MI";
		minSize = 10;
		criteria.push_back(2);
		strcpy(outputFile,"./output/output.csv");

	}else{
		strcpy(inputFile, argv[1]);
		strcpy(outputFile,argv[2]);
		keyword = argv[3];
		minSize = atoi(argv[4]);
		for(int i = 5; i < argc ; i++){
			int attr = atoi(argv[i]);
			if(attr < 0 || attr > 7) return 0;
			criteria.push_back(atoi(argv[i]));
		}
	}
	
	// validate the parameters
	if(minSize <= 0) return 0;
	if(criteria.size() == 0) return 0;
	

	// print the inputs
	cout << "Input parameters "	<< endl;
	cout << "	input file name: " << inputFile << endl;
	cout << "	output file name: " << outputFile << endl;
	cout << "	keyword: " << keyword << endl;
	cout << "	minsize: " << minSize << endl;
	cout << "	number of attributes: " << criteria.size() << endl;
	cout << "	attributes: " ;
	for(int i = 0; i < criteria.size(); i++)
		cout << attributeList[criteria[i]] << " ";
	cout << endl;
	return 1;
}


/*
function name - main
task - takes the user input, creates threads, and waits for the threads to finish the tasks 
    programname input_file_name output_file_name keyword minsize criteria
    0 - id, 1 - gender, 2 - firstname, 3 - middlename, 4 - lastname, 5 - cityname, 6 - statename, 7 - ssn
*/

int main(int argc, char **argv)
{
        pthread_t readerThread, sorterThread, writerThread;
	int numRecords, minSize, ret;
	char inputFile[30];
	char outputFile[30];
	vector<int> criteria;
	string keyword;
	retVal *rVal1, *rVal2, *rVal3;
	struct timeval now, later;

	int success = validateParams(argc, argv, inputFile, outputFile, keyword, minSize, criteria);
	if(!success)
		return 0;
	
	rVal1 = new retVal();
	argListforRW* readArg = new argListforRW();
	readArg->fileName = inputFile;
	readArg->rVal = rVal1;

	// add codes to set the attributes and create the main 3 threads - readerThread, sorterThread, writerThread
	ret = pthread_create(&readerThread, NULL, readData, (void *) readArg);
	if(ret)
	{
		cout << "Reader Thread could not be created." << endl;
		exit(EXIT_FAILURE);
	}
	

	
	rVal2 = new retVal();
	argList* sortArg = new argList();
	sortArg->startIndex = 0;
	sortArg->endIndex = 0;
	sortArg->keyword = keyword;
	sortArg->threadno = 0;
	sortArg->minSize = minSize;
	sortArg->criteria = criteria;
	sortArg->rVal = rVal2;
	

	
	ret = pthread_create(&sorterThread, NULL, mergesort, (void *) sortArg);
	if(ret)
	{
		cout << "Sorter Thread could not be created." << endl;
		exit(EXIT_FAILURE);
	}
	
	rVal3 = new retVal();
	argListforRW* writeArg = new argListforRW();
	writeArg->fileName = outputFile;
	writeArg->rVal = rVal3;
	
	ret = pthread_create(&writerThread, NULL, writeData, (void *) writeArg);
	if(ret)
	{
		cout << "Writer Thread could not be created." << endl;
		exit(EXIT_FAILURE);
	}

	// add codes to print the results, do not print if there is no record in the database 
	pthread_join(readerThread, NULL);
	pthread_join(sorterThread, NULL);
	pthread_join(writerThread, NULL);
	
	cout << "Output:" << endl;
	cout <<"\tTotal CPU time for reading : "<< rVal1->timeUsed << " microseconds" << endl;
	cout <<"\tTotal CPU time for sorting : "<< rVal2->timeUsed << " microseconds" << endl;
	cout <<"\tTotal CPU time for writing : "<< rVal3->timeUsed << " microseconds" << endl;
	cout <<"\tKeyword found: " << rVal2->frequency << " times" << endl;
	// add codes to clean the allocated memory
	
	delete readArg;
	delete rVal1;
	delete sortArg;
	delete rVal2;
	delete writeArg;
	delete rVal3;
	
	pthread_cond_destroy(&con);
	pthread_mutex_destroy(&mut);
	pthread_exit(NULL);
}
