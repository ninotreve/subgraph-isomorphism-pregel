#ifndef SICANDIDATE_H
#define SICANDIDATE_H

typedef hash_map<int, hash_set<SIKey> > Candidate;
// Candidate Model: AND-OR TREE
class SICandidate
{
public:
    // typedef hash_map<int, hash_set<SIKey> > Candidate;
    // candidates[curr_u][next_u] = vector<SIKey>
    hash_map<int, Candidate> candidates;
    // curr_u: vector<next_u>
    hash_map<int, vector<int> > cand_map;

    void fillInvalidSet(hash_set<int> &invalid_set)
    {
    	int u1, u2;
		for (auto cand_it = cand_map.begin(); cand_it != cand_map.end();
				cand_it ++)
		{
			u1 = cand_it->first;
			for (int u : cand_it->second)
			{
				if (candidates[u1][u].empty())
				{
					invalid_set.insert(u1);
					break;
				}
			}
		}
    }

    bool hasCandidates() { return !candidates.empty(); }

};

#endif