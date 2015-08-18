#include <signal.h>
#include <unistd.h>
#include <sstream>
#include <cstdlib>
#include <cstdio>

#include "proj1.h"
using namespace std;

/*
	function name: int_handler - handles  interrupt signal and take necessary action
	input: signal number
	output: none
*/

void int_handler(int signo)
{
	if(signo == SIGINT)
   	{
       	alarm(0); // Stop SIGALRM
       	string choice;

       	cout << "\nSelect one of the following: \nQ to Quit \nI to change free memory checking Interval \nT to change the Threshold  \nK to kill the receiver process\nC to Continue \nWaiting for user input:\n";
       	cin >> choice;
        
       	// Ask for inputs until the user enters a valid response
       	while(true)
       	{
           	if(choice[0] == 'Q' || choice[0] == 'q')
           	{
			  // handle quit case
                  exit(signo); // works hooray
           	}
           	else if(choice[0] == 'I' || choice[0] == 'i')
       		{
			  // get new interval and continue
		  cout << "Enter new interval (in seconds), if a negative value is entered it will be taken as a postive, also if an interval of 0 is entered the program will idle until a new value is entered." << endl;
		  cin >> interval;
                  // check to make sure the interval entered is a number, if not set to zero
		  /*if()
                  {
                      interval = 0;
                  }
*/
                  if(interval < 0)
                  {
                      // if the interval is negative make it positive
                      interval *= -1; 
                  }

		  // not sure why, but sometimes cout takes awhile to run. 
		  cout << "Interval is set to: " << interval << " seconds" << endl;
		  break;
		}
       		else if(choice[0] == 'T' || choice[0] == 't')
       		{
			  // get new threshold and continue
		  // putting in check to make sure it's a number that is entered and not a letter, if it's a letter things break
		  cout << "Enter new threshold (in kB), if a negative value is entered it will be made positive." << endl; 
		  cin >> threshold; 
	         
                  if(threshold < 0)
                  {
                      // if the interval is negative make it positive
                      threshold *= -1; 
                  }

		  cout << "Threshold is set to: " << threshold << " kB's" << endl;
		  break;
       		}
       		else if(choice[0] == 'K' || choice[0] == 'k')
       		{
			  // find the receiver program (if it is running) and terminate it.
			  // define function in header and call it here 
                          kill_receiver();
			  break;
                }
       		else if(choice[0] == 'C' || choice[0] == 'c')
       		{
			  // continue sampling of free memory
		  break; 
		}
       		else
       		{
			  cout << "\nSelect one of the following: \nQ to Quit \nI to change free memory checking Interval \nT to change the Threshold  \nK to kill the receiver program\nC to Continue \nWaiting for user input:\n";
				  cin >> choice;
       		}		
      	}
	}
        // check if the signal is interrupt and if not output error message
	else
	{
            cout << "Error, SIGINT not found";
        }
// need to reset the alarm on exit form this loop
alarm(interval);
}


/*
	function name: alarm_handler - handles alarm signal
	input: signal no
	output: none
*/	
void alarm_handler(int signo)
{
  // report free memory status with check_free_memory() and restart alarm
  
  //check to make sure it was the alarm sent in 
  if(signo == SIGALRM)
  {
      check_free_memory();
      alarm(interval); 
  }
  else 
  {
      cout << "Error SIGALRM not found" << endl;
  }
}

/* 
	main function
*/
int main(int argc, char* argv[])
{
  
  cout << "Monitoring Free Memory." << endl;
  cout << " Default frequency is " << interval << " seconds." << endl;
  cout << " Default threshold is " << threshold << " kB." << endl << endl;
   

  // register the signal handlers (SIGINT, SIGALARM)
  if(signal(SIGALRM, alarm_handler) == SIG_ERR) 
  {
    cout << "failed to register alarm handler." << endl;
    exit(EXIT_FAILURE);
  }

  if(signal(SIGINT, int_handler) == SIG_ERR)
  {
    cout << "failed to register interrupt handler." << endl;
    exit(EXIT_FAILURE);
  }

  // set the first alarm 
  alarm(interval);
  while(true)
   {
     sleep(10);
   }
}
