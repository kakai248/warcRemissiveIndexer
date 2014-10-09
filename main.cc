#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

#define WORK_TAG 1
#define STOP_TAG 2
 
using namespace std;

map<string, map<string, int> > keywords_by_website;

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
	printf ("Number of tasks= %d My rank= %d Running on %s\n", numtasks,rank,hostname);

	// Read file
	ifstream file;
	string line;
	file.open(argv[1]);

	long file_pos;
	void *buf = malloc(sizeof(long));

	if(rank == 0) {
		file.seekg(0, file.end);
		long length = file.tellg();

		long part = length/(numtasks-1);
		memcpy(buf, &part, sizeof(long));

		MPI_Scatter(buf, 1, MPI_LONG, &file_pos, 1, MPI_LONG, 0, MPI_COMM_WORLD);
	}
	else {
		MPI_Scatter(buf, 1, MPI_LONG, &file_pos, 1, MPI_LONG, 0, MPI_COMM_WORLD);

		long start_pos = file_pos * (rank-1);
		long end_pos = start_pos + file_pos;

		std::string prefix_lf("WARC/1.0");
		std::string prefix("WARC-Target-URI: ");

		if (file.is_open()) {
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

						// adds word to website
						map<string, int> *words_for_website = &keywords_by_website[url];
						for(vector<string>::const_iterator i = words.begin(); i != words.end(); ++i) {
							if(words_for_website->find(*i) == words_for_website->end()) {
								words_for_website->insert(std::pair<string, int>(*i, 1));
							}
							else words_for_website->at(*i) = words_for_website->at(*i) + 1;
						}

					}
				}
			}	
			file.close();
		}
	}

	// Gather
	if(rank == 0) {
		int stop_counter = 0;
		MPI_Status status;

		while(stop_counter < numtasks) {
			MPI_Recv(void *buf, int count, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// add to map

			if(status.MPI_TAG == STOP_TAG) {
      			stop_counter++;
    		}
		}
	}
	else {
		for(map<string, map<string, int> >::const_iterator i = keywords_by_website.begin(); i != keywords_by_website.end();) {
			map<string, map<string, int> >::const_iterator cur = i++;

			std::string str = (*cur).first + " ";
			for(map<string, int>::const_iterator j = cur.begin(); j != cur.end(); ++j) {
				str += (*j).first + " " + (*j).second + " ";
			}

			// Send info
			if(i != keywords_by_website.end()) {
				MPI_Send(str.c_str(), str.size(), MPI_CHAR, 0, WORK_TAG, MPI_COMM_WORLD);
			}
			else {
				MPI_Send(str.c_str(), str.size(), MPI_CHAR, 0, STOP_TAG, MPI_COMM_WORLD);
			}
		}
	}

	MPI_Finalize();
}
