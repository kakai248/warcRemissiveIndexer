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

int main(int argc, char *argv[]) {
	// Checks if there is a file name
	if(argc < 3) {
		cout << "usage: " << argv[0] << " input_file output_file" << endl;
		exit(0);
	}

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

				// ignore 7 lines, ie, starts reading the document words
				for(int i = 0; i < 7; i++) getline (file,line);

				// for each line of words for this document
				while(getline(file, line) && line.substr(0, prefix_lf.size()) != prefix_lf) {
					vector<string> words = split(line, ' ');

					// adds website to word
					for(vector<string>::const_iterator i = words.begin(); i != words.end(); ++i) {
						string s = string(*i);
						map<string, int> *websites = &keywords[trim(s)];

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

	cout << "Outputing to file..." << endl;

	// Output to file
	ofstream out;
	out.open(argv[2]);

	for(map<string, map<string, int> >::const_iterator i = keywords.begin(); i != keywords.end(); ++i) {
		out << (*i).first << " :" << endl;
		for(map<string, int>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
			out << "\t" << intToString(j->second) << " : " << j->first << endl;
		}
		out << endl << endl;
	}
	out.close();
}
