#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <list>

using namespace std;

map<string, string> keywords;

ofstream out;

/* -------------- */

void flush(map<string, string>::iterator &i) {
	out << (*i).first << endl;
	out << (*i).second << endl;

	out << endl << endl;
}

void writeToFile() {
	for(map<string, string>::iterator i = keywords.begin(); i != keywords.end(); ++i) {
		flush(i);
	}
}

int main(int argc, char *argv[]) {
	// Test arguments
	if(argc < 3) {
		cout << "usage: " << argv[0] << " output_file input_file1 [input_file2] ..." << endl;
		exit(0);
	}

	// output file
	string output_file = argv[1];

	// process each input file
	for(int i = 2; i < argc; i++) {
		cout << "Processing input file: " << argv[i] << endl;

		// Read file
		ifstream file;
		string line;
		file.open(argv[i]);

		if (file.is_open()) {
			// while we don't reach the end of the file
			while ( getline (file,line)) {
				// skip empty lines
				if(line.empty()) continue;
				
				// when it stops being empty, its a word
				string word = line;
				
				// process next lines, ie, process counter + url
				while (getline (file,line) && !line.empty()) {
					string word_details = line;

					keywords[word].push_back(*word_details.c_str());
				}
			}
			file.close();
		}
	}

	// Output to file
	out.open(output_file.c_str());
	cout << "Outputting to file..." << endl;
	writeToFile();
	out.close();
}
