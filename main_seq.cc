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
 
using namespace std;

map<string, map<string, int> > keywords;

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

void filter(string &s) {
	if(s[s.size()-1] == '\r') s = s.substr(0, s.length()-1);
}

bool validWord(string &s) {
	return !s.empty() && (s[0] >= 65 && s[0] <= 90 || s[0] >= 97 && s[0] <= 122);
}

void flush(map<string, map<string, int> >::iterator &i) {
	out << (*i).first << " :" << endl;
	for(map<string, int>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
		out << "\t" << intToString(j->second) << " : " << j->first << endl;
	}
	out << endl << endl;
}

void writeToFile() {
	for(map<string, map<string, int> >::iterator i = keywords.begin(); i != keywords.end(); ++i) {
		flush(i);
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
	if(argc == 3)
		output_file = argv[2];

	// Read file
	ifstream file;
	string line;
	file.open(argv[1]);

	std::string prefix_lf("WARC/1.0");
	std::string prefix("WARC-Target-URI: ");

	cout << "Processing input..." << endl;

	if (file.is_open()) {
		// while we don't reach the end of the file
		while ( getline (file,line)) {

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
						map<string, int> *websites = &keywords[word];

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

	// Output to file
	out.open(output_file.c_str());
	cout << "Outputing to file..." << endl;
	writeToFile();
	out.close();
}
