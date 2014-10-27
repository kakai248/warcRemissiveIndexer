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

struct Reference {
	string url;
	int counter;
};

map<string, list<Reference> > keywords;

ofstream out;

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

void flush(map<string, list<Reference> >::iterator &i) {
	out << (*i).first << endl;
	for(list<Reference>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
		out << intToString(j->counter) << " " << j->url << endl;
	}
	out << endl << endl;
}

void writeToFile() {
	for(map<string, list<Reference> >::iterator i = keywords.begin(); i != keywords.end(); ++i) {
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
					vector<string> s = split(line, ' ');
					
					Reference r;
					r.url = s[1];
					r.counter = atoi(s[0].c_str());
					keywords[word].push_back(r);
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
