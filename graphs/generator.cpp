#include <iostream>
#include <vector>
#include <cstdlib> // Header file needed to use srand and rand
#include <ctime> // Header file needed to use time

using namespace std;

int main(int argc, char* argv[])
{
    if (argc != 6)
    {
        cout << "not enough arguments" << endl;
        return -1;
    }
    bool isGraph = (argv[1][0] == 'g');
    int n = atoi(argv[2]); // number of vertices
    int m = atoi(argv[3]); // number of edges
    int l = atoi(argv[4]); // number of labels
    int seed = atoi(argv[5]); // seed

    if (m < n - 1)
    {
        cout << "m must be greater or equal to n - 1" << endl;
        return -1;
    }
    if (m > n * (n-1) / 2)
    {
        cout << "m is too large" << endl;
        return -1;
    }

    srand(seed);

    // generate random labels
    vector<int> labels = vector<int>(n);
    for (int i = 0; i < n; i++)
        labels[i] = 97 + rand() % l;

    bool edges[n*n];
    for (int i = 0; i < n*n; i++)
        edges[i] = 0;

    // generate a tree (each vertex must be connected to a previous vertex)
    for (int i = 1; i < n; i++)
    {
        int o = rand() % i;
        edges[i*n + o] = 1;
        edges[o*n + i] = 1;
    }
    // generate other edges randomly
    int spaces = n * n - n - 2 * (n - 1);
    for (int e = 0; e < m - n + 1; e++)
    {
        int place = rand() % spaces;
        int index = 0;
        for (int i = 0; i < n; i++)
        {
            for (int j = 0; j < n; j++)
            {
                if (i == j || edges[i*n + j]) continue;
                if (index == place)
                {
                    edges[i*n + j] = 1;
                    edges[j*n + i] = 1;
                    index ++;
                }
                else
                    index ++;
            }
        }
        spaces -= 2;
    }

    int check_count = 0;
    for (int i = 0; i < n*n; i++)
        if (edges[i]) check_count ++;

    if (check_count != m * 2)
    {
        cout << "wrong result" << endl;
        return -1;
    }

    // print out
    if (isGraph)
    {
        for (int i = 0; i < n; i++)
        {
            cout << i << " " << (char) labels[i] << "\t";
            for (int j = 0; j < n; j++)
                if (edges[i*n + j])
                    cout << j << " " << (char) labels[j] << " ";
            cout << endl; 
        }
    }
    else
    {
        for (int i = 0; i < n; i++)
        {
            cout << i << " " << labels[i] << " ";
            int count = 0;
            for (int j = 0; j < n; j++)
                if (edges[i*n + j])
                    count ++;

            cout << count << " ";
            for (int j = 0; j < n; j++)
                if (edges[i*n + j])            
                    cout << j << " ";
            cout << endl; 
        }        
    }
	return 0;
}