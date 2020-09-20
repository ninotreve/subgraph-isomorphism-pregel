#ifndef SIKEY_H
#define SIKEY_H

//----------------SIKey = <VertexID, WorkerID, partial_mapping>----------

struct SIKey;
typedef vector<SIKey> Mapping;

struct SIKey {
    VertexID vID;
    int wID;
    Mapping partial_mapping;

    SIKey()
    {
    }

    SIKey(int v, int w)
    {
    	this->vID = v;
    	this->wID = w;
    }

    SIKey(int v, int w, Mapping & partial_mapping)
    {
        this->vID = v;
        this->wID = w;
        this->partial_mapping = partial_mapping;
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
        return (vID == rhs.vID) && (partial_mapping == rhs.partial_mapping);
    }

    inline bool operator!=(const SIKey& rhs) const
    {
        return (vID != rhs.vID) || (partial_mapping != rhs.partial_mapping);
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
    m << v.partial_mapping;
    return m;
}

obinstream& operator>>(obinstream& m, SIKey& v)
{
    m >> v.vID;
    m >> v.wID;
    m >> v.partial_mapping;
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
	        size_t seed = 0;
	        hash_combine(seed, key.vID);
	        for (SIKey &k : key.partial_mapping)
	        	hash_combine(seed, k.vID);
	        return seed;
		}
	};
}

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

#endif