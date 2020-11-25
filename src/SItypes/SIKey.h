#ifndef SIKEY_H
#define SIKEY_H

//----------------SIKey = <VertexID, WorkerID>-----------------------
// the same implementation as vwpair

struct SIKey {
    int vID;
    int wID;

    SIKey()
    {
    }

    SIKey(int v, int w)
    {
    	this->vID = v;
    	this->wID = w;
    }

    inline bool operator<(const SIKey& rhs) const
    {
        return (vID < rhs.vID);
    }

    inline bool operator>(const SIKey& rhs) const
    {
        return (vID > rhs.vID);
    }

    inline bool operator==(const SIKey& rhs) const
    {
        return (vID == rhs.vID);
    }

    inline bool operator!=(const SIKey& rhs) const
    {
        return (vID != rhs.vID);
    }

    int hash()
    {
    	return wID;
    }
};

ibinstream& operator<<(ibinstream& m, const SIKey& v)
{
    m << v.vID;
    m << v.wID;
    return m;
}

obinstream& operator>>(obinstream& m, SIKey& v)
{
    m >> v.vID;
    m >> v.wID;
    return m;
}

class SIKeyHash {
public:
    inline int operator()(SIKey key)
    {
    	// this hash for partitioning vertices
        return key.hash();
    }
};

namespace __gnu_cxx {
	template <>
	struct hash<SIKey> {
		size_t operator()(SIKey key) const
		{
			// this is general hash
	        return key.vID;
		}
	};
}

/*
// Define hash of Mapping

namespace __gnu_cxx {
	template <>
	struct hash<Mapping> {
		size_t operator()(Mapping m) const
		{
			size_t seed = 0;
			for (SIKey &k : m)
				hash_combine(seed, k.vID);
			return seed;
		}
	};
}
*/
#endif