#include<iostream>
#include<string>
#include<fstream>
#include<sstream>
#include<vector>
#include<map>
using namespace std;

class DBSystem
{
	int psize,npages;
	string path;
	vector<string> pages;// Pages are here 
	map<string,pair<int,int> > table_map; // gives a mapping from name of the table to the start and the end page ID
	vector<pair<int,int> > page_map; // page_map[0] gives the start and the end record Id in the page 0
	map<int,int> mem_pages; // <pageId,freq> of pages in the memory
	public:
	string getRecord(string name,int id);
	string search(string page,int recordId);
	void readConfig(string filename);
	void populatepageInfo();
	void insertRecord(string tableName, string record);
	void flushPages();
};

void DBSystem::readConfig(string filename) //stores the metadata of the tables and their attributes in the meta.txt file .
{
	ifstream conf (filename.c_str());
	ofstream meta ("meta.txt");
	string ll,s1,s2;
	int fl=0;
	if(conf.is_open())
	{
		while(getline(conf,ll))
		{
			stringstream line,temp;
			line << ll;
			line >> s1 >> s2;
			temp << s2;
			if(!s1.compare("PAGESIZE"))
				temp >> psize;
			else if(!s1.compare("NUM_PAGES"))
				temp >> npages;
			else if(!s1.compare("PATH_FOR_DATA"))
				temp >> path;
			else if(!s1.compare("BEGIN"))
				fl=1;
			else if(!s1.compare("END"))
			{
				fl=0;
				meta << '\n';
			}
			else
				meta << s1 << ' ';
		}
	}
	cout << psize << ' ' << npages << ' ' << path << '\n';
	conf.close();
	meta.close();
}

void DBSystem::populatepageInfo()
{
	ifstream meta ("meta.txt");
	string ll;
	int page_num=0;
	while(getline(meta,ll))// each line in the meta file contains details about one table 
	{
		pages.push_back(string(""));
		stringstream line;
		string s1;
		int i,record_no=0,f_point=0;//record_no helps in counting the recordIds of a particular table irrespective of the pages that they are stored in and f_point helps to track the end of the page and not accidently writing a record by which the max page size exceeds .
		line << ll;
		line >> s1;
		ifstream file ((s1+".csv").c_str());//the corresponding .csv files contain the records
		string ll2;
		file.is_open();
		table_map[s1]=pair<int,int>(page_num,page_num);//store the start and the end page as (0,0) for the first table
		page_map.push_back(pair<int,int>(record_no,record_no));//in the page 0 store (0,0) for initial and the final recordIds
		while(getline(file,ll2))
		{
			stringstream outpage,out2;
			string rec_header,rec;//rec contains the contents of the record and the rec_header contains the header details
			out2 << 0;//NOTE : The rec_header does not the total lenght details initially . it is added later .
			rec_header.append(out2.str()+'#');//every record's first field is at 0.
			for(i=0;i<ll2.length();i++)
			{
				if(ll2[i]==',')
				{
					stringstream out;
					out << i+1;
					rec_header.append(out.str()+'#');
				}
				else if(ll2[i]=='\n')//this will never arise actually as each record spans only one line and '\n' is not included in ll2
				{
				}
				else
				{
					stringstream out;
					out << ll2[i];
					rec.append(out.str());
				}

			}
			rec.append("$");//end of record symbol
			stringstream out3;
			out3 << i << '#' << rec_header << rec;//out3 is used for calculating the length of the entire record
			outpage  << out3.str().length() << '#' << rec_header << rec;//record length is appended to header info here
			if(outpage.str().length()+f_point > psize)//details of outpage are entered only when it fits into current page
			{
				//I enter in this only if the record does not fit into the current page
				page_map[page_num].second=record_no-1;//previous pages end record_no
				page_map.push_back(pair<int,int>(record_no,record_no));//next page's initial and final recordIds

				page_num++;
				pages.push_back(string(""));//new empty page is created
				f_point=0;
			}
			//incrementing the page_num is sufficient to indicate new page . Everything is as usual
			record_no++;
			pages[page_num].append(outpage.str());//appending the record present in outpage to the current page .
			f_point+=outpage.str().length();
		}
		f_point=0;
		page_map[page_num].second = record_no-1;
		record_no=0;
		table_map[s1].second=page_num;//since I will go to next table itself in the next iteration . Enter the previous pages end pageId here.
		page_num++;//Since new table has to be entered in a new page so here we create a new page
		file.close();//close the current table's .csv file 

	}
	meta.close();
	cout << "Here is page wise Printing(change the page size to get different results)" << '\n';

	for(int j=0;j<pages.size();j++)
		cout << pages[j] << '\n';

}

string DBSystem::getRecord(string tableName,int recordId)
{
	
	int startPage = table_map[tableName].first;
	int endPage = table_map[tableName].second;//take the start and the end page Ids of the table with name==tablename
	int pageReq;//pageReq is the required page that contains the relevent information .
	for(int i=startPage;i<=endPage;i++)
		if(page_map[i].first <= recordId && page_map[i].second >= recordId )
		{
			pageReq = i;
		}
	
	map<int,int>::iterator it=mem_pages.find(pageReq);
	if(it!=mem_pages.end())//enter this loop if the pageReq is present already in the mem_pages map .
	{
		cout << "HIT" << '\n';
		mem_pages[pageReq]++;//increment the frequency of access
	}
	else
	{
		cout << "MISS" << '\n';
		if(mem_pages.size()==npages)//enter this loop only if we have to replace any of the existing pages 
		{
			map<int,int>::iterator least=mem_pages.begin();
			map<int,int>::iterator it=++least;
			least--;
			for(;it!=mem_pages.end();it++)
				if(it->second < least->second)
					least=it;
			mem_pages.erase(least->first);//least iterator will point to the page that needs to be removed.

		}
		mem_pages.insert(pair<int,int>(pageReq,1));

	}
	return search(pages[pageReq],recordId-page_map[pageReq].first);//search for the required record in the page that contains it

}

string DBSystem::search(string page,int recordId)
{
	int index=0;//will finally contain the starting index of the header of the required record .
	int length;//stores the various record lengths in the page.
	string result;
	while(recordId!=0)
	{
		stringstream buf;
		buf << page.substr(index);//make a sub string of page from index till the end .
		buf >> length;//get the length of the current record
		index+=length;
		recordId--;

	}
	stringstream buf;
	buf << page.substr(index);
	buf >> length;
	return page.substr(index,length);//make the subtring from index till the end of the record .    
}

void DBSystem::insertRecord(string tableName, string record)
{

}

void DBSystem::flushPages()
{

}

int main()
{
	DBSystem mine;
	mine.readConfig("config.txt");
	mine.populatepageInfo();
	while(1)
	{
		stringstream query;
		string mystr,name;
		int id;
		getline(cin,mystr);
		query << mystr;
		query >> name >> id;
		cout << mine.getRecord(name,id) << '\n';
	}
	return 0;
}

