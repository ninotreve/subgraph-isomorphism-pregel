#ifndef SIAGG_H
#define SIAGG_H

typedef vector<vector<double>> AggMat;

class SIAgg : public Aggregator<AggMat, AggMat>
{
	// uniform aggregator for candidates and mappings
	// agg_mat[u1, u1] = candidate(u1);
	// agg_mat[u1, u2] = sum_i(|C'_{u1, vi}(u2)|), u1 < u2
	// agg_mat[0, 0] = # mappings
public:
	AggMat agg_mat;

    virtual void init()
    {
		agg_mat.resize(3);
		for (int i = 0; i < 3; ++i)
		{
			agg_mat[i].resize(3);
			for (int j = 0; j < 3; ++j)
				agg_mat[i][j] = 0.0;
		}
    }

    virtual void stepFinal(AggMat* part)
    {
    	for (int i = 0; i < part->size(); ++i)
			for (int j = 0; j < (*part)[i].size(); ++j)
    			agg_mat[i][j] += (*part)[i][j];
    }

    virtual AggMat* finishPartial()
    {
    	return &agg_mat;
    }

    virtual AggMat* finishFinal()
    {
    	return &agg_mat;
    }

    void addTime(int index_x, int index_y, double time)
    {
        agg_mat[index_x][index_y] += time;
    }

    void addMappingCount(int index_x, int index_y, long count)
    {
        agg_mat[index_x][index_y] += count;
    }
};

#endif