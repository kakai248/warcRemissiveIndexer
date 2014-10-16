#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm> 
#include <functional> 
#include <cctype>
#include <locale>

#define MAIN_WORKER 0
#define WORK_TAG 1
#define STOP_TAG 2
 
using namespace std;

map<string, map<string, int> > keywords;

/* Utility functions */

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

std::string intToString(int number) {
	ostringstream ss;
	ss << number;
	return ss.str();
}

/* -------------- */

void writeToFile(string file_name) {
	// Output to file
	ofstream out;
	out.open(file_name.c_str());

	for(map<string, map<string, int> >::const_iterator i = keywords.begin(); i != keywords.end(); ++i) {
		out << (*i).first << " :" << endl;
		for(map<string, int>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
			out << "\t" << intToString(j->second) << " : " << j->first << endl;
		}
		out << endl << endl;
	}
	out.close();
}

void addToMap(string s) {
	vector<string> vs = split(s, ' ');

	// adds website to word
	string word = vs[0];
	cout << "word: " << word << endl;

	for(int i = 1; i < vs.size(); i += 2) {
		map<string, int> *websites = &keywords[word];
		string url = vs[i];
		cout << "url: " << url << endl;
		cout << "size: " << vs.size() << " " << i << endl;

		if(websites->find(url) == websites->end())
			(*websites)[url] = atoi(vs[i+1].c_str());
		else
			(*websites)[url] += atoi(vs[i+1].c_str());
	}
}

int main(int argc, char *argv[]) {
	// Test arguments
	if(argc < 2) {
		cout << "usage: " << argv[0] << " input_file [output_file]" << endl;
		exit(0);
	}

	// Check if there is a output file
	string output_file = "out.txt";
	if(argc >= 2)
		output_file = argv[3];

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

	// Read filelen

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

		std::string prefix_lf("WARC/1.0");
		std::string prefix("WARC-Target-URI: ");

		cout << "Processing, rank " << rank << endl;

		if (file.is_open()) {
			// goes to this process position
			file.seekg(start_pos);

			// while we don't reach the end of the file
			while ( getline (file,line) && file.tellg() <= end_pos) {

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
							map<string, int> *websites = &keywords[*i];

							if(websites->find(url) == websites->end())
								(*websites)[url] = 1;
							else
								(*websites)[url]++;
						}
					}
				}
			}	
			file.close();
		}
	}

	MPI_Barrier(MPI_COMM_WORLD);

	// Gather results
	if(rank == 0) {
		int stop_counter = 0;
		MPI_Status status;

		cout << "Receiving, rank " << rank << endl;

		// While it doesnt receive a final tag from all workers
		while(stop_counter < numtasks-1) {
			int size;
			MPI_Status status;

			// Probe for an incoming message from any zero
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// Get the size of the message
			MPI_Get_count(&status, MPI_CHAR, &size);
			char *buf = new char[size];

			MPI_Recv(buf, size, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// Add to map
			addToMap(string(buf));

			if(status.MPI_TAG == STOP_TAG) {
      			stop_counter++;
    		}
		}
	}
	else {
		cout << "Sending, rank " << rank << endl;

		for(map<string, map<string, int> >::const_iterator i = keywords.begin(); i != keywords.end(); ) {
			map<string, map<string, int> >::const_iterator cur = i++;

			// Form string to send to master
			string str = "";
			str += cur->first;
			for(map<string, int>::const_iterator j = cur->second.begin(); j != cur->second.end(); ++j) {
				str += " " + j->first + " " + intToString(j->second);
			}

			// Send info
			if(i != keywords.end()) {
				MPI_Send(const_cast<char *>(str.c_str()), str.length(), MPI_CHAR, MAIN_WORKER, WORK_TAG, MPI_COMM_WORLD);
			}
			else {
				cout << "Final send, rank " << rank << endl;
				MPI_Send(const_cast<char *>(str.c_str()), str.length(), MPI_CHAR, MAIN_WORKER, STOP_TAG, MPI_COMM_WORLD);
			}
		}
	}

	// Output to file
	if(rank == 0) {
		cout << "Outputing to file..." << endl;
		//writeToFile(output_file);
	}

	MPI_Finalize();
}
