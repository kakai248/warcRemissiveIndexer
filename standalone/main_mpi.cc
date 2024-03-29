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
#include <unistd.h>
#include <iterator>
#include <list>

#define MAIN_WORKER 0
#define WORK_TAG 1
#define STOP_TAG 2

#define WORKER_MEM_FREE_CYCLE 10000
 
using namespace std;

struct Reference {
	string url;
	int counter;
};

map<string, list<Reference> > main_keywords;
map<string, map<string, int> > worker_keywords;

int numtasks;

ofstream out;

char main_last_worker_letter = 0;
int main_last_worker_counter = 0;

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

void filter(string &s) {
	if(s[s.size()-1] == '\r') s = s.substr(0, s.length()-1);
}

bool validWord(string &s) {
	return !s.empty() && (s[0] >= 65 && s[0] <= 90 || s[0] >= 97 && s[0] <= 122);
}

void flush(map<string, list<Reference> >::iterator &i) {
	out << (*i).first << endl;
	for(list<Reference>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
		out << intToString(j->counter) << " " << j->url << endl;
	}
	out << endl << endl;
}

void writeToFile() {
	for(map<string, list<Reference> >::iterator i = main_keywords.begin(); i != main_keywords.end(); ++i) {
		flush(i);
	}
}

void dump(char c) {
	for(map<string, list<Reference> >::iterator i = main_keywords.begin(); i != main_keywords.end();) {
		// if we are in a word that starts with a character bigger than c
		if((*i).first[0] > c) break;
	
		flush(i);
		
		// Delete element
		i->second.clear();
		main_keywords.erase(i++);
	}
}

void checkPartialDump(string &word) {
	if(word[0] > main_last_worker_letter)
		main_last_worker_counter++;
		
	if(main_last_worker_counter >= numtasks-1) {
		cout << "Outputing word " << word << endl;
		main_last_worker_letter = word[0];
		main_last_worker_counter = 0;
		dump(word[0]);
	}
}

void addToMap(string &s) {
	vector<string> vs = split(s, ' ');

	// adds website to word
	string word = vs[0];

	for(int i = 1; i < vs.size(); i += 2) {
		string url = vs[i];
		
		Reference r;
		r.url = url;
		r.counter = atoi(vs[i+1].c_str());
		main_keywords[word].push_back(r);
	}
	
	checkPartialDump(word);
}

int main(int argc, char *argv[]) {
	// Test arguments
	if(argc < 2) {
		cout << "usage: " << argv[0] << " input_file [output_file]" << endl;
		exit(0);
	}

	// Check if there is a output file
	string output_file = "out.txt";
	if(argc == 3)
		output_file = argv[2];

	int  rank, len, rc;
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
					filter(url);

					// ignore 7 lines, ie, starts reading the document words
					for(int i = 0; i < 7; i++) getline (file,line);

					// for each line of words for this document
					while(getline(file, line) && line.substr(0, prefix_lf.size()) != prefix_lf) {
						vector<string> words = split(line, ' ');

						// adds website to word
						for(int i = 0; i < words.size(); i++) {
							string word = words[i];
							if(!validWord(word)) continue;
							filter(word);

							// get url map for this string
							map<string, int> *websites = &worker_keywords[word];

							if(websites->find(url) == websites->end())
								(*websites)[url] = 1;
							else
								(*websites)[url]++;
						}
						
						words.clear();
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
		
		// Open output file
		out.open(output_file.c_str());

		cout << "Receiving, rank " << rank << endl;

		// While it doesnt receive a final tag from all workers
		while(stop_counter < numtasks-1) {
			int size;
			MPI_Status status;

			// Probe for an incoming message from any zero
			MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// Get the size of the message
			MPI_Get_count(&status, MPI_CHAR, &size);
			char buf[size];

			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

			// Add to map
			if(size > 0) {
				string s = string(buf, size);
				addToMap(s);
			}

			if(status.MPI_TAG == STOP_TAG) {
      			stop_counter++;
    		}
		}
	}
	else {
		cout << "Sending, rank " << rank << endl;

		int cycle_counter = 0;

		for(map<string, map<string, int> >::iterator i = worker_keywords.begin(); i != worker_keywords.end(); ) {
			map<string, map<string, int> >::iterator cur = i++;

			// Form string to send to master
			string str = "";
			str += cur->first;
			for(map<string, int>::const_iterator j = cur->second.begin(); j != cur->second.end(); ++j) {
				str += " " + j->first + " " + intToString(j->second);
			}
			
			// delete the words up to current
			if(cycle_counter++ == WORKER_MEM_FREE_CYCLE) {
				worker_keywords.erase(worker_keywords.begin(), cur);
				cycle_counter = 0;
			}

			// Send info
			if(i != worker_keywords.end()) {
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
		cout << "Outputting to file..." << endl;
		writeToFile();
		out.close();
	}

	MPI_Finalize();
}
