#include<iostream>
#include<stdio.h>
#include<unistd.h>
#include<dirent.h>
#include<stdlib.h>
#include<errno.h>
#include<malloc.h>
#include<vector>
#include <streambuf>
#include <fstream>
using namespace std;

struct Task{
    string taskName;
    string taskID;
    string logContent;


    double init;
    double inputMap;
    double inputReader;
    double processor;
    double sink;
    double spill;
    double spillInteral;
    double shuffle;
    double output;
    double outputInteral;
    double SQLoperator;

    double startTime;
    double endTime;
    double taskTime;

    int runNode;

};

double nsGapToms(double start, double end){
    return (end - start)/1000000.0;
}

double nsToms(double time){
    return time/1000000.0;
}

int lastPosofStr(string log, string word){
    int index = 0;
    int pos = 0;
    while ((index = log.find(word, index)) < log.length()){
        if(index > 0)
            pos = index;
        index++;
    }
    return pos;
}


void extractTaskNode(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxl = task[i].logContent.find("on dbg");
        if(idxl != string::npos){
            string tmpNode = task[i].logContent.substr(idxl + 6, 2);
            task[i].runNode = stoi(tmpNode);
        }
    }
}



void extractTaskName(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxl = task[i].logContent.find("task info: VectorName: ");
        string::size_type idxr = task[i].logContent.find(" VertexParallelism:");
        int rlen = sizeof("task info: VectorName: ");
        if(idxl != string::npos  &&  idxr != string::npos){
            string tmpTaskName = task[i].logContent.substr(idxl + rlen -1, idxr - (idxl + rlen -1));
            task[i].taskName = tmpTaskName;
        }
    }
}

void extractTaskID(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxl = task[i].logContent.find("TaskID: ");
        int rlen = sizeof("TaskID: ");
        if(idxl != string::npos){
            int move = idxl + rlen -1;
            while(task[i].logContent[move] != '\n'){
                move++;
            }
            string tmpTaskID = task[i].logContent.substr(idxl + rlen -1, move - (idxl + rlen -1));
            task[i].taskID = tmpTaskID;
        }
    }
}

void extractInittime(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        //std::cout << "Task name: " << task[i].taskName << "||\n";

        string::size_type idxl = task[i].logContent.find("Profiling: Tez 'Initialization'");
        string::size_type idxr = task[i].logContent.find("take time ");
        int rlen = sizeof("take time ");
        if(idxl != string::npos  &&  idxr != string::npos){
            int move = idxr + rlen -1;
            while(task[i].logContent[move] != '\n'){
                move++;
            }
            string tmpInit = task[i].logContent.substr(idxr + rlen -1, move - (idxr + rlen -1));
            task[i].init = stod(tmpInit);
        }
        //std::cout << "      Init take time: " << task[i].init << "xx\n";
    }
}

void extractInputMap(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxTask = task[i].taskName.find("Map");
        if(idxTask != string::npos) {
            //std::cout << "Task name: " << task[i].taskName << "||\n";

            double start = 0;
            double end = 0;
            double timeInit = 0;
            double timeReader = 0;

            string::size_type idxStart = task[i].logContent.find("Profiling: Tez 'Input' on Map stage starting at time ");
            int rlen = sizeof("Profiling: Tez 'Input' on Map stage starting at time ");
            if(idxStart != string::npos){
                int move = idxStart + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idxStart + rlen -1, move - (idxStart + rlen -1));
                start = stod(tmpStart);
                //std::cout << "      Input start time: " << stod(tmpStart) << "**\n";
            }


            string::size_type idxEnd = task[i].logContent.find("Profiling: Tez 'Input' on Map stage ending at time ");
            rlen = sizeof("Profiling: Tez 'Input' on Map stage ending at time ");
            if(idxEnd != string::npos){
                int move = idxEnd + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpend = task[i].logContent.substr(idxEnd + rlen -1, move - (idxEnd + rlen -1));
                end = stod(tmpend);
                //std::cout << "      Input end time: " << end << "**\n";
            }

            timeInit = nsGapToms(start, end);
            //std::cout << "      Input timepart1 time: " << timeInit << " ms\n";

            string sub = "Profiling: Tez 'Input' continue to take time ";
            rlen = sizeof("Profiling: Tez 'Input' continue to take time ");
            int lastpos = lastPosofStr(task[i].logContent, sub);
            if(lastpos > 0){
                int move = lastpos + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string temcon = task[i].logContent.substr(lastpos + rlen -1, move - (lastpos + rlen -1));
                timeReader = stod(temcon);
                //std::cout << "      Input reader time: " << temcon << " ms\n";
            }

            task[i].inputMap = timeInit + timeReader;
            task[i].inputReader = timeReader;
            //std::cout << "      Input on Map take time: " << task[i].inputMap << " ms\n";
            //std::cout << "      inputReader on Map take time: " << task[i].inputReader << " ms\n";
        }
    }
}


void extractProcessor(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxTaskMap = task[i].taskName.find("Map");
        string::size_type idxTaskReduce = task[i].taskName.find("Reducer");
        if(idxTaskMap != string::npos) {
            //std::cout << "Task name: " << task[i].taskName << "||\n";

            double start = 0;
            double end = 0;

            string::size_type idxStart = task[i].logContent.find("Profiling: Tez 'Processor' on Map stage starting at time ");
            int rlen = sizeof("Profiling: Tez 'Processor' on Map stage starting at time ");
            if(idxStart != string::npos){
                int move = idxStart + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idxStart + rlen -1, move - (idxStart + rlen -1));
                start = stod(tmpStart);
                //std::cout << "      Processor start time: " << stod(tmpStart) << "**\n";
            }

            string::size_type idxEnd = task[i].logContent.find("Profiling: Tez 'Processor' on Map stage ending at time ");
            rlen = sizeof("Profiling: Tez 'Processor' on Map stage ending at time ");
            if(idxEnd != string::npos){
                int move = idxEnd + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpEnd = task[i].logContent.substr(idxEnd + rlen -1, move - (idxEnd + rlen -1));
                end = stod(tmpEnd);
                //std::cout << "      Processor end time: " << stod(tmpEnd) << "**\n";
            }

            task[i].processor = nsGapToms(start, end);
            //std::cout << "      Processor cost time: " << task[i].processor << "**\n";
        }

        if(idxTaskReduce != string::npos) {
            //std::cout << "Task name: " << task[i].taskName << "||\n";

            double start = 0;
            double end = 0;

            string::size_type idxStart = task[i].logContent.find("Profiling: Tez 'Processor' on Reduce stage starting at time ");
            int rlen = sizeof("Profiling: Tez 'Processor' on Reduce stage starting at time ");
            if(idxStart != string::npos){
                int move = idxStart + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idxStart + rlen -1, move - (idxStart + rlen -1));
                start = stod(tmpStart);
                //std::cout << "      Processor start time: " << stod(tmpStart) << "**\n";
            }

            string::size_type idxEnd = task[i].logContent.find("Profiling: Tez 'Processor' on Reduce stage ending at time ");
            rlen = sizeof("Profiling: Tez 'Processor' on Reduce stage ending at time ");
            if(idxEnd != string::npos){
                int move = idxEnd + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpEnd = task[i].logContent.substr(idxEnd + rlen -1, move - (idxEnd + rlen -1));
                end = stod(tmpEnd);
                //std::cout << "      Processor end time: " << stod(tmpEnd) << "**\n";
            }

            task[i].processor = nsGapToms(start, end);
            //std::cout << "      Processor cost time: " << task[i].processor << "**\n";
        }
    }
}


void extractSink(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        //std::cout << "Task name: " << task[i].taskName << "||\n";
        double timeSink = 0;
        int rlen;

        auto* sub = new string [2];
        sub[0] = "Profiling: Tez 'Sink' takes time ";
        sub[1] = "Profiling: Tez 'Sink' at Reduce stage take time ";
        int model = 0;
        int lastPos = -1;
        for(int m = 0; m < 2; m++){
            string tmp = sub[m];
            lastPos = lastPosofStr(task[i].logContent, sub[m]);
            if(lastPos > 0) {
                model = m;
                break;
            }
        }

        if(lastPos > 0){
            //std::cout << "      Sink model is " << model << "\n";

            if(model == 0){
                rlen = sizeof("Profiling: Tez 'Sink' takes time ");
            }

            if(model == 1){
                rlen = sizeof("Profiling: Tez 'Sink' at Reduce stage take time ");
            }

            int move = lastPos + rlen -1;
            while(task[i].logContent[move] != ' '){
                move++;
            }
            string tmpSink = task[i].logContent.substr(lastPos + rlen -1, move - (lastPos + rlen -1));
            timeSink = stod(tmpSink);

        }

        task[i].sink = timeSink;
        //std::cout << "      Sink cost time: " << timeSink << "**\n";
    }
}


void extractSpill(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxTaskMap = task[i].taskName.find("Map");
        if(idxTaskMap != string::npos) {
            //std::cout << "Task name: " << task[i].taskName << "||\n";
            double timeCont = 0;
            double timeSpill = 0;

            double startCont;
            double endCont;
            string::size_type idxCont = task[i].logContent.find("Profiling: Tez 'Spill' on Map stage continuing at time ");
            int rlen = sizeof("Profiling: Tez 'Spill' on Map stage continuing at time ");
            if(idxCont != string::npos){
                int move = idxCont + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpContStart = task[i].logContent.substr(idxCont + rlen -1, move - (idxCont + rlen -1));
                startCont = stod(tmpContStart);
            }

            idxCont = task[i].logContent.find("Profiling: Tez 'Spill' at Map stage ending at time ");
            rlen = sizeof("Profiling: Tez 'Spill' at Map stage ending at time ");
            if(idxCont != string::npos){
                int move = idxCont + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpContStart = task[i].logContent.substr(idxCont + rlen -1, move - (idxCont + rlen -1));
                endCont = stod(tmpContStart);
            }

            timeCont = nsGapToms(startCont, endCont);



            string sub = "Profiling: Tez 'Spill' at Map stage partly take time: ";
            int lastPos = lastPosofStr(task[i].logContent, sub);
            if(lastPos > 0){
                rlen = sizeof("Profiling: Tez 'Spill' at Map stage partly take time: ");
                int move = lastPos + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpSpill = task[i].logContent.substr(lastPos + rlen -1, move - (lastPos + rlen -1));
                timeSpill = stod(tmpSpill);
            }


            task[i].spill = timeCont + timeSpill;
            task[i].spillInteral = timeSpill;
            //std::cout << "      Spill timeCont: " << timeCont << " ms\n";
            //std::cout << "      Spill time: " << timeSpill << " ms\n";
            //std::cout << "      Spill total time: " << task[i].spill << " ms\n";
            //std::cout << "      Spill spillInteral time: " << task[i].spillInteral << " ms\n";
        }
    }
}



void extractTaskTime(vector<Task>& task){

    vector<double> taskStart;

    for(int i = 0; i < task.size(); i++){

        double start = 0;
        double end = 0;

        string::size_type idxStart = task[i].logContent.find("Profiling: Tez Container task starting at time ");
        int rlen = sizeof("Profiling: Tez Container task starting at time ");
        if(idxStart != string::npos){
            int move = idxStart + rlen -1;
            while(task[i].logContent[move] != ' '){
                move++;
            }
            string tmpStart = task[i].logContent.substr(idxStart + rlen -1, move - (idxStart + rlen -1));
            start = stod(tmpStart);
        }

        taskStart.push_back(start);
        task[i].startTime = start;

        string::size_type idxEnd = task[i].logContent.find("Profiling: Tez Container task ending at time ");
        rlen = sizeof("Profiling: Tez Container task ending at time ");
        if(idxEnd != string::npos){
            int move = idxEnd + rlen -1;
            while(task[i].logContent[move] != ' '){
                move++;
            }
            string tmpEnd = task[i].logContent.substr(idxEnd + rlen -1, move - (idxEnd + rlen -1));
            end = stod(tmpEnd);
        }
        task[i].endTime = end;
    }

    double globalStart = INT64_MAX;
    for(int i = 0; i < taskStart.size(); i++){
        if(taskStart[i] < globalStart)
            globalStart = taskStart[i];
    }

    for(int i = 0; i < task.size(); i++){
        //std::cout << "Task name: " << task[i].taskName << "||\n";

        double gap = task[i].endTime - task[i].startTime;
        task[i].startTime = task[i].startTime - globalStart;
        task[i].endTime = task[i].startTime + gap;
        task[i].taskTime = gap;
        //std::cout << "  Task start at: " << task[i].startTime << " ms\n";
        //std::cout << "  Task end at: " << task[i].endTime << " ms\n";
        //std::cout << "  Task cost time: " << task[i].taskTime << " ms\n";
    }
}



void extractShuffle(vector<Task>& task){
    for(int i = 0; i < task.size(); i++){
        string::size_type idxTask = task[i].taskName.find("Reducer");
        if(idxTask != string::npos) {
            //std::cout << "Task name: " << task[i].taskName << "||\n";

            double start = 0;
            double end = 0;

            string::size_type idx = task[i].logContent.find("Profiling: Tez 'Shuffle' on Reduce stage starting at time ");
            int rlen = sizeof("Profiling: Tez 'Shuffle' on Reduce stage starting at time ");
            if(idx != string::npos){
                int move = idx + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idx + rlen -1, move - (idx + rlen -1));
                start = stod(tmpStart);
            }

            idx = task[i].logContent.find("Profiling: Tez 'Shuffle' on Reduce stage ending at time ");
            rlen = sizeof("Profiling: Tez 'Shuffle' on Reduce stage ending at time ");
            if(idx != string::npos){
                int move = idx + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idx + rlen -1, move - (idx + rlen -1));
                end = stod(tmpStart);
            }

            task[i].shuffle = nsGapToms(start, end);
            //std::cout << "  Shuffle start at: " << start << " ms\n";
            //std::cout << "  Shuffle end at: " << end << " ms\n";
            //std::cout << "  Shuffle cost time: " << task[i].shuffle << " ms\n";
        }
    }
}


void extractOutput(vector<Task>& task){
    for(int i = 0; i < task.size(); i++) {
        string::size_type idxTask = task[i].taskName.find("Reducer");
        if (idxTask != string::npos) {
            //std::cout << "Task name: " << task[i].taskName << "||\n";

            double timeCont = 0;
            double timeOut = 0;

            double startCont;
            double endCont;
            string::size_type idx = task[i].logContent.find("Profiling: Tez 'Output' on Reduce stage continuing at time ");
            int rlen = sizeof("Profiling: Tez 'Output' on Reduce stage continuing at time ");
            if(idx != string::npos){
                int move = idx + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idx + rlen -1, move - (idx + rlen -1));
                startCont = stod(tmpStart);
            }

            idx = task[i].logContent.find("Profiling: Tez 'Output' at Reduce stage ending at time ");
            rlen = sizeof("Profiling: Tez 'Output' at Reduce stage ending at time ");
            if(idx != string::npos){
                int move = idx + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpStart = task[i].logContent.substr(idx + rlen -1, move - (idx + rlen -1));
                endCont = stod(tmpStart);
            }

            timeCont = nsGapToms(startCont, endCont);

            string sub = "Profiling: Tez 'Output' at Reduce stage partly take time: ";
            int lastPos = lastPosofStr(task[i].logContent, sub);
            if(lastPos > 0){
                rlen = sizeof("Profiling: Tez 'Output' at Reduce stage partly take time: ");
                int move = lastPos + rlen -1;
                while(task[i].logContent[move] != ' '){
                    move++;
                }
                string tmpOut = task[i].logContent.substr(lastPos + rlen -1, move - (lastPos + rlen -1));
                timeOut = stod(tmpOut);
            }

            task[i].output = timeCont + timeOut;
            task[i].outputInteral = timeOut;
            //std::cout << "  Output timeCont at: " << timeCont << " ms\n";
            //std::cout << "  Output timeOut at: " << timeOut << " ms\n";
            //std::cout << "  Output cost time: " << task[i].output << " ms\n";
            //std::cout << "  Output internal time: " << task[i].outputInteral << " ms\n";
        }
    }
}


void extractSQLoperator(vector<Task>& task){
    for(int i = 0; i < task.size(); i++) {
        string::size_type idxTask = task[i].taskName.find("Map");
        if (idxTask != string::npos) {
            std::cout << "Task name: " << task[i].taskName << "||\n";
            task[i].SQLoperator = task[i].processor - task[i].inputReader - task[i].sink - task[i].spillInteral;
            if(task[i].SQLoperator < 0)
                task[i].SQLoperator = 0;
            //std::cout << task[i].SQLoperator << " = " << task[i].processor << " - " << task[i].inputReader << " - " << task[i].sink << " - " << task[i].spillInteral << "\n";
        }

        idxTask = task[i].taskName.find("Reducer");
        if (idxTask != string::npos) {
            std::cout << "Task name: " << task[i].taskName << "||\n";
            task[i].SQLoperator = task[i].processor - task[i].sink - task[i].outputInteral;
            if(task[i].SQLoperator < 0)
                task[i].SQLoperator = 0;
            //std::cout << task[i].SQLoperator << " = " << task[i].processor << " - " << task[i].sink << " - " << task[i].outputInteral << "\n";
        }
    }
}

int main() {
    FILE *fp = NULL;
    char *file = "info.log";
    char *line = NULL;
    size_t line_len = 1024;

    vector<Task> task;
    string tmpLog;

    if ((0 != access(file, R_OK | F_OK)) || (NULL == (fp = fopen(file, "r")))) {
        printf("open %s failed, errno=%d\n", file, errno);
        return -1;
    }
    while (getline(&line, &line_len, fp) > 0){
        string tmpLine = line;
        string::size_type idxe = tmpLine.find("Profiling: Tez Container task ending at time");
        //new task log
        tmpLog += tmpLine;
        if(idxe != string::npos){
            tmpLog += tmpLine;
            Task tmpTask;
            tmpTask.logContent = tmpLog;
            task.push_back(tmpTask);
            tmpLog = "";
        }
    }
    if (fp != NULL) {
        fclose(fp);
    }
    if (line) {
        free(line);
    }

    extractTaskNode(task);
    extractTaskName(task);
    extractTaskID(task);
    extractInittime(task);
    extractInputMap(task);
    extractProcessor(task);
    extractSink(task);
    extractSpill(task);
    extractTaskTime(task);
    extractShuffle(task);
    extractOutput(task);
    extractSQLoperator(task);

    /*for(int i = 0; i < task.size(); i++){
        std::cout << "\n===================================================================\n";

        std::cout << "Task name: " << task[i].taskName << "||\n";
        std::cout << "Task ID: " << task[i].taskID << "||\n";
        std::cout << "Init time: " << task[i].init << "||\n";

        //std::cout << task[i].logContent << "||\n";
    }*/

    std::ofstream ofile;
    ofile.open("Profile.csv", ios::out | ios::trunc);
    ofile << "Map Container Profile (unit:ms)"  << endl;
    ofile << "TaskName" << "," << "TaskID"  << "," << "Initailization" << "," << "Input" << "," << "SQLoperator" << "," << "Sink"
            << "," << "Spill" << "," << "Starting Time" << "," << "Ending Time" << "," << "Cost Time" << "," << "Node Locate" <<"\n";

    for(int i = 0; i < task.size(); i++){
        string::size_type idxTask = task[i].taskName.find("Map");
        if (idxTask != string::npos) {
            ofile << task[i].taskName << "," << task[i].taskID  << "," << task[i].init << "," << task[i].inputMap << "," << task[i].SQLoperator
            << "," << task[i].sink << "," << task[i].spill << "," << task[i].startTime << "," << task[i].endTime << "," << task[i].taskTime
            << "," << task[i].runNode << endl;
        }
    }

    ofile << endl << endl << endl;
    ofile << "Reduce Container Profile (unit:ms)"  << endl;
    ofile << "TaskName" << "," << "TaskID"  << "," << "Initailization" << "," << "Shuffle&Merge" << "," << "SQLoperator" << ","<< "Sink"
            << "," << "Output" << "," << "Starting Time" << "," << "Ending Time" << "," << "Cost Time" << "," << "Node Locate" <<"\n";

    for(int i = 0; i < task.size(); i++){
        string::size_type idxTask = task[i].taskName.find("Reducer");
        if (idxTask != string::npos) {
            ofile << task[i].taskName << "," << task[i].taskID  << "," << task[i].init << "," << task[i].shuffle << "," << task[i].SQLoperator << "," << task[i].sink
                    << "," << task[i].output << "," << task[i].startTime << "," << task[i].endTime << "," << task[i].taskTime
                    << "," << task[i].runNode << endl;
        }
    }

    ofile.close();



    return 0;
}
