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

// trim from start
static inline std::string &ltrim(std::string &s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
	return s;
}
// trim from end
static inline std::string &rtrim(std::string &s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
	return s;
}
// trim from both ends
static inline std::string &trim(std::string &s) {
	return ltrim(rtrim(s));
}

/* -------------- */

string filter(string s) {
	string str = trim(s);
	str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
	str.erase(std::remove(str.begin(), str.end(), '\r'), str.end());
	return str;
}

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
				std::string url = filter(line.substr(prefix.size(), line.size()));

				// ignore 7 lines, ie, starts reading the document words
				for(int i = 0; i < 7; i++) getline (file,line);

				// for each line of words for this document
				while(getline(file, line) && line.substr(0, prefix_lf.size()) != prefix_lf) {
					vector<string> words = split(line, ' ');

					// adds website to word
					for(int i = 0; i < words.size(); i++) {
						string word = filter(words[i]);
						if(word.empty()) continue;

						// get url map for this string
						map<string, int> *websites = &keywords[word];

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

	// Output to file
	cout << "Outputing to file..." << endl;
	writeToFile(output_file);
}
