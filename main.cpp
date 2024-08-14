#include <iostream>
#include <vector>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <unordered_map>
// #include <filesystem>
#include <dirent.h>
#include <utility>

#define HGINT long long
const int H_READ = 0;
const int H_WRITE = 1;

struct Data {              
    char md5[33]; 
    HGINT  start;
    HGINT  end;
};

class HGDB
{
public:
    HGDB(std::string name, int type);
    ~HGDB(){
        if (h_type == H_WRITE)
        {
            commit();
            std::cout << "commit" << std::endl;
        }
        if (h_data.is_open())
        {
            h_data.close();
        }
        if (h_idx.is_open())
        {
            h_idx.close();
        }
        if (h_idx_read.is_open())
        {
            h_idx_read.close();
        } 
        if(mmap_buffer)
        {
            munmap(mmap_buffer, mmap_buffer_len);
            std::cout << "close HGDB" << std::endl;
        }
    };

    void put(const std::string md5, const std::string databin);
    std::string find(const std::string md5);
    std::string get(const std::string md5);
    void show();
    void commit();
private:
    int length;
    char hgnp[33];
    std::string hgnp_str;
    std::string folderPath;
    std::string data_file;
    std::string idx_file;
    Data idx_data;
    int h_type;
    char * mmap_buffer = nullptr;
    HGINT mmap_buffer_len;
    std::string nullchar = "";
    std::ofstream h_data;
    std::fstream h_idx;
    std::ifstream h_idx_read;
    std::unordered_map<std::string, std::pair<HGINT, HGINT>> idx_maps;
};

HGDB::HGDB(std::string name, int type)
{
    char default_str[33]="90b58287d96484fd9a33cc56d4e690a3";
    strcpy(hgnp, default_str);
    hgnp_str = default_str;
    folderPath=name;
    h_type = type;
    if(h_type == H_READ)
    {
        struct stat buffer;   
        if (stat(name.c_str(), &buffer) == 0)
        {
            data_file = folderPath + "/data.hgdb";
            idx_file = folderPath + "/idx.hgdb";
            if (!(stat(data_file.c_str(), &buffer) == 0))
            {
                std::cout << data_file << " not found" << std::endl;
                return;
            }
            if (!(stat(idx_file.c_str(), &buffer) == 0))
            {
                std::cout << idx_file << "not found" << std::endl;
            }
            h_idx_read.open(idx_file, std::ios::in);
            Data da;
            while(h_idx_read.read((char *)&da, sizeof(Data)))
            {
                std::pair<HGINT, HGINT> first_pair(da.start, da.end);
                std::string insert_name(da.md5);
                idx_maps.insert(std::make_pair(insert_name, first_pair));
            }
            h_idx_read.close();

            int fd = open(data_file.c_str(), O_RDONLY);
            mmap_buffer_len = lseek(fd, 0, SEEK_END);
            mmap_buffer = (char *) mmap(NULL, mmap_buffer_len, PROT_READ, MAP_SHARED, fd, 0);
            close(fd);
        } else {
             std::cout << folderPath << " not found" << std::endl;
        }
    }
    else if (h_type == H_WRITE)
    {
        struct stat buffer;
        if (stat(name.c_str(), &buffer) == 0)
        {
            data_file = folderPath + "/data.hgdb";
            idx_file = folderPath + "/idx.hgdb";
            if ((stat(data_file.c_str(), &buffer) == 0) and (stat(idx_file.c_str(), &buffer) == 0))
            {
            
                h_idx_read.open(idx_file, std::ios::in);
                Data da;
                while(h_idx_read.read((char *)&da, sizeof(Data)))
                {
                    std::pair<HGINT, HGINT> first_pair(da.start, da.end);
                    std::string insert_name(da.md5);
                    idx_maps.insert(std::make_pair(insert_name, first_pair));
                }
                h_idx_read.close();
                h_data.open(data_file, std::ios::app | std::ios::binary);
                h_idx.open(idx_file, std::ios::in | std::ios::out | std::ios::binary);
                h_idx.seekp(0, std::ios::end);
            }
            else
            {
                std::cout << data_file << " or " << idx_file << " not found" << std::endl;
                return;
            }
        }
        else
        {
            if (mkdir(name.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) 
            {
                std::cout << "create " << folderPath << " failed" << std::endl;
                return;
            }
            else
            {
                std::cout << "create " << folderPath << " success" << std::endl;
            }
            data_file = folderPath + "/data.hgdb";
            idx_file = folderPath + "/idx.hgdb";
            std::ofstream file_create(idx_file);
            file_create.close();

            h_data.open(data_file, std::ios::app | std::ios::binary);
            if (h_data.is_open()==0)
            {
                std::cout << "open "<< data_file << " failed"<<std::endl;
                return;
            }

            h_idx.open(idx_file, std::ios::out | std::ios::in | std::ios::binary);
            if (h_idx.is_open()==0)
            {
                std::cout << "open "<< idx_file << " failed"<<std::endl;
                return;
            }
            const char first_char = 'H';
            h_data.write(&first_char, 1);
            std::pair<HGINT, HGINT> first_pair(0, 1);
            std::string first_name(hgnp_str);
            idx_maps.insert(std::make_pair(first_name, first_pair));
            Data da;
            memcpy(&da.md5, hgnp, 33);
            da.start = 0;
            da.end = 1;
            h_idx.write((const char*)&da, sizeof(Data));
        }
    }
}

void HGDB::put(const std::string md5, const std::string databin)
{
    h_data.write(databin.data(), databin.size());
    HGINT max_indx = idx_maps[hgnp_str].second;

    Data da;
    strcpy(da.md5, md5.c_str());
    da.start = max_indx;
    da.end = max_indx+databin.size();
    idx_maps[hgnp_str].second = max_indx+databin.size();
    h_idx.write((const char*)&da, sizeof(Data));
    std::string img_md5(md5);
    idx_maps.insert(std::make_pair(img_md5, std::pair<HGINT, HGINT>(max_indx, idx_maps[hgnp_str].second)));
}

void HGDB::commit()
{
    h_idx.seekp(0, std::ios::beg);
    Data da;
    memcpy(&da.md5, hgnp, 33);
    da.start = idx_maps[hgnp_str].first;
    da.end = idx_maps[hgnp_str].second;
    h_idx.write((const char*)&da, sizeof(Data));
    h_idx.seekp(0, std::ios::end);
}

std::string HGDB::get(const std::string md5)
{
    std::string res;
    res.assign(mmap_buffer+idx_maps[md5].first, mmap_buffer+idx_maps[md5].second);
    return res;
}

std::string HGDB::find(const std::string md5)
{
    std::unordered_map<std::string, std::pair<HGINT, HGINT>>::iterator iter = idx_maps.find(md5);
    if(iter != idx_maps.end())
    {
        std::string res;
        res.assign(mmap_buffer+iter->second.first, mmap_buffer+iter->second.second);
        return res;
    }
    else
    {
        std::cout << md5 << " not found" << std::endl;
        return nullchar;
    }
}

void HGDB::show()
{
    std::unordered_map<std::string, std::pair<HGINT, HGINT>>::iterator iter;
    for(iter = idx_maps.begin(); iter != idx_maps.end(); iter++)
    {
        std::cout<<iter->first<<" "<<iter->second.first << " " << iter->second.second<<std::endl;
    }
}


