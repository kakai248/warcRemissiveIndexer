#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
 
using namespace std;

map<string, map<string, int> > keywords_by_website;

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

int main(int argc, char *argv[]) {
	int  numtasks, rank, len, rc; 
	/*char hostname[MPI_MAX_PROCESSOR_NAME];

	rc = MPI_Init(&argc,&argv);
	if (rc != MPI_SUCCESS) {
		printf ("Error starting MPI program. Terminating.\n");
		MPI_Abort(MPI_COMM_WORLD, rc);
	}

	MPI_Comm_size(MPI_COMM_WORLD,&numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	MPI_Get_processor_name(hostname, &len);
	printf ("Number of tasks= %d My rank= %d Running on %s\n", numtasks,rank,hostname);*/

	/* Read file */
	ifstream file;
	string line;
	file.open("file.internal.warc.wet");
	
	std::string prefix_lf("WARC/1.0");
	std::string prefix("WARC-Target-URI: ");

	if (file.is_open()) {
	    	while ( getline (file,line) ) {
				if(line.substr(0, prefix.size()) == prefix) {
	    			std::string url = line.substr(prefix.size(), line.size());
					for(int i = 0; i < 7; i++) getline (file,line);

					getline(file, line);
					while(line.substr(0, prefix_lf.size()) != prefix_lf) {
						vector<string> words = split(line, ' ');

						map<string, int> *words_for_website = &keywords_by_website[url];
						for(vector<string>::const_iterator i = words.begin(); i != words.end(); ++i) {
							if(words_for_website->find(*i) == words_for_website->end()) {
								words_for_website->insert(std::pair<string, int>(*i, 1));
							}
							else words_for_website->at(*i) = words_for_website->at(*i) + 1;
						}

						getline(file, line);
					}
				}
	    	}
	    	file.close();
	}

	cout << "Number of websites: " << keywords_by_website.size();

	//MPI_Finalize();
}
