#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

#define MAIN_WORKER 0
#define WORK_TAG 1
#define STOP_TAG 2
 
using namespace std;

map<string, map<string, int> > keywords;

/* Split functions */
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}
std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

std::string intToString(int number)
  {
     ostringstream ss;
     ss << number;
     return ss.str();
  }

/* -------------- */

int main(int argc, char *argv[]) {
	// Checks if there is a file name
	if(argc < 1) {
		cout << "usage: " << argv[0] << " filename" << endl;
		exit(0);
	}

	int  numtasks, rank, len, rc;
	char hostname[MPI_MAX_PROCESSOR_NAME];

	rc = MPI_Init(&argc,&argv);
	if (rc != MPI_SUCCESS) {
		printf ("Error starting MPI program. Terminating.\n");
		MPI_Abort(MPI_COMM_WORLD, rc);
	}

	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Get_processor_name(hostname, &len);

	// Read file
	ifstream file;
	string line;
	file.open(argv[1]);

	long file_pos;

	// Rank 0 divides the file in parts and sends a part to each worker
	if(rank == 0) {
		file.seekg(0, file.end);
		long length = file.tellg();

		long part = length/(numtasks-1);

		MPI_Bcast(&part, 1, MPI_LONG, MAIN_WORKER, MPI_COMM_WORLD);
	}
	// Each worker receives the file part and processes it
	else {
		MPI_Bcast(&file_pos, 1, MPI_LONG, MAIN_WORKER, MPI_COMM_WORLD);		

		long start_pos = file_pos * (rank-1);
		long end_pos = start_pos + file_pos;

		printf ("Broadcast rcv, My rank= %d, file_pos: %ld, start_pos: %ld, end_pos: %ld\n", rank, file_pos, start_pos, end_pos);

		std::string prefix_lf("WARC/1.0");
		std::string prefix("WARC-Target-URI: ");

		if (file.is_open()) {
			printf("Rank= %d opened file\n", rank);			

			// goes to this process position
			file.seekg(start_pos);

			// while we don't reach the end of the file
			while ( getline (file,line) && file.tellg() >= end_pos) {

				// if we find a uri, ie, a new document
				if(line.substr(0, prefix.size()) == prefix) {
					std::string url = line.substr(prefix.size(), line.size());

					// ignore 7 lines, ie, starts reading the document words
					for(int i = 0; i < 7; i++) getline (file,line);

					// for each line of words for this document
					while(getline(file, line) && line.substr(0, prefix_lf.size()) != prefix_lf) {
						vector<string> words = split(line, ' ');

						// adds website to word
						for(vector<string>::const_iterator i = words.begin(); i != words.end(); ++i) {
							map<string, int> *word = &keywords[*i];
							
							if(word->find(url) == word->end()) {
								word->insert(std::pair<string, int>(url, 1));
							}
							else word->at(url) = word->at(url) + 1;
						}

					}
				}
			}	
			file.close();
		}
	}

	// Gather results
	if(rank == 0) {
		int stop_counter = 0;
		MPI_Status status;

		printf ("Receiving: My rank= %d Running on %s\n", rank, hostname);

		while(stop_counter < numtasks) {
			int size;
			MPI_Status status;

			// Probe for an incoming message from any zero
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// Get the size of the message
			MPI_Get_count(&status, MPI_CHAR, &size);
			char *buf = new char[size];

			MPI_Recv(buf, size, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// TODO: add to map

			if(status.MPI_TAG == STOP_TAG) {
      			stop_counter++;
    		}
		}
	}
	else {
		printf ("Sending: My rank= %d Running on %s\n", rank, hostname);

		for(map<string, map<string, int> >::const_iterator i = keywords.begin(); i != keywords.end(); ) {
			//map<string, map<string, int> >::const_iterator cur = i++;

			printf ("Processing!");

			std::string str = (*i).first + " ";
			for(map<string, int>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
				printf ("Processing! concat");
				str += (*j).first + " " + intToString((*j).second) + " ";
			}

			i++;

			// Send info
			if(i != keywords.end()) {
				printf ("Sending!");
				MPI_Send(&str, str.length(), MPI_CHAR, MAIN_WORKER, WORK_TAG, MPI_COMM_WORLD);
			}
			else {
				printf ("final Sending!");
				MPI_Send(&str, str.length(), MPI_CHAR, MAIN_WORKER, STOP_TAG, MPI_COMM_WORLD);
			}
		}
	}

	MPI_Finalize();
}
