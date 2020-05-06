

#include <iostream>
#include <boost/program_options.hpp>

using namespace std;

namespace po = boost::program_options;


int main(int argc, char **argv) {
    int retval = 0;
    std::cout << " In " __FILE__ "\n";

  po::options_description desc("Allowed options");
  int var2;
  desc.add_options()
    ("help,h", "produce help message")
    ("var1", po::value<int>(), "set var1")
    ("var2", po::value<int>(&var2)->default_value(10), "set var2")
    ("string1", po::value<string>(), "set string1")
    ("list1", po::value< vector<string> >(), "set list1")
    ("input-file", po::value< vector<string> >(), "set input files")
    ;
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  }



    return retval;
}