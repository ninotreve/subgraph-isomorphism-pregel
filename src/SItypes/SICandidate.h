#ifndef SICANDIDATE_H
#define SICANDIDATE_H

#include "../basic/MessageBuffer.h"
// Candidate Model: AND-OR TREE
class SICandidate
{
public:
	// used in the filtering step:
	// candidates[curr_u][neighbor_u] = hash_set<SIKey>
	vector<int> cand_us;
	vector<vector<hash_set<SIKey>>> candidates;

    size_t getSize()
    {
        return this->candidates.size();
    }

    size_t getSize(int i, int j) 
    {
        return this->candidates[i][j].size();
    }

    int getCandidate(int i)
    {
        return this->cand_us[i];
    }

    int getInverseIndex(int curr_u)
    {
        int i;
        for (i = 0; this->cand_us[i] != curr_u; i++);
        return i;
    }

};

#endif