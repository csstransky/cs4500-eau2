// Written by Kaylin Devchand & Cristian Stransky
#pragma once

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

const char* DEFAULT_CLIENT_IP = "127.0.0.2";
const char* DEFAULT_SERVER_IP = "127.0.0.1";
const char* DEFAULT_TEXT_FILE = "data/words.txt";
const int DEFAULT_NODE_INDEX = 0;

// TODO: A little better input command getter, but there are issues if this is done:
// ./client -o -s -ip 10.0.0.1
const char* get_arg(int argc, char const *argv[], const char* flag, int num_args) {
    for (int ii = 0; ii < argc; ii++)
        if (strcmp(argv[ii], flag) == 0 && ii + num_args < argc)
        return argv[ii + num_args];
    return nullptr;
}

const char* get_input_client_ip_address(int argc, char const *argv[]) {
    const char* arg = get_arg(argc, argv, "-ip", 1);
    if (arg)
        return arg;
    else {
        printf("If you wish to choose an IP for the client, use:\n");
        printf("-ip <IP address>\n\n");
        return DEFAULT_CLIENT_IP;
    }
}

const char* get_input_server_ip_address(int argc, char const *argv[]) {
    const char* arg = get_arg(argc, argv, "-s", 1);
    if (arg) {
        return arg;
    }
    else {
        printf("If you wish to choose an IP for the server, use:\n");
        printf("-s <Server IP address>\n\n");
        return DEFAULT_SERVER_IP;
    }
}

const char* get_input_text_file(int argc, char const *argv[]) {
    const char* arg = get_arg(argc, argv, "-o", 1);
    if (arg)
        return arg;
    else {
        printf("If you wish to choose the text file to read, use:\n");
        printf("-o <text file>\n\n");
        return DEFAULT_TEXT_FILE;
    }
}

int get_input_node_index(int argc, char const *argv[]) {
    const char* arg = get_arg(argc, argv, "-n", 1);
    if (arg)
        return atoi(arg);
    else {
        printf("If you wish to choose the node index, use:\n");
        printf("-n <node index>\n\n");
        return DEFAULT_NODE_INDEX;
    }
}