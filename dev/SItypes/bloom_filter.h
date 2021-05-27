#ifndef INCLUDE_BLOOM_FILTER_HPP
#define INCLUDE_BLOOM_FILTER_HPP

// #include <iostream>
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <string>
#include <vector>
#include <utility>

static const std::size_t bits_per_char = 0x08;    // 8 bits in 1 char(unsigned) 

static const unsigned int minimum_number_of_hashes = 3;
static const unsigned int maximum_number_of_hashes = 128;
static const unsigned int minimum_size = 10;
static const unsigned long int maximum_size = std::numeric_limits<unsigned long int>::max();
//minimum & maximum constraints


static const unsigned char bit_mask[bits_per_char] = {
                                                       0x01,  //00000001
                                                       0x02,  //00000010
                                                       0x04,  //00000100
                                                       0x08,  //00001000
                                                       0x10,  //00010000
                                                       0x20,  //00100000
                                                       0x40,  //01000000
                                                       0x80   //10000000
                                                     };

class bloom_filter
{
protected:
	//type rename
   	typedef unsigned int bloom_type;
   	typedef std::vector<unsigned char> table_type;
   	typedef std::pair<int,int> element_type;	//can be a template as well
	
public:
	//Generating function
	/* the member in a bloom filter:
		salt_count_ - k, the number of hash functions, 
		table_size_ - m, the length of bit table
		projected_element_count_ - n
		inserted_element_count_ - count the current element in filter
		desired_false_positive_probability_ - f, the probability give out a false positive result
	*/ 
   bloom_filter()
   : salt_count_(0),
     table_size_(0),
     projected_element_count_(0),
     inserted_element_count_ (0),
     random_seed_(0)
   {}

   virtual ~bloom_filter()
   {}

   void init(double     false_positive_probability,
   			 unsigned long long int random_seed)
   {
      inserted_element_count_ = 0;
      random_seed_ = random_seed * 0xA5A5A5A5 + 1;
      desired_false_positive_probability_ = false_positive_probability;
   	compute_optimal_parameters();	//compute the optimal salts and table size
      salt_count_ = optimal_parameters.number_of_hashes;
      table_size_ = optimal_parameters.table_size;

      generate_unique_salt();	//generate salts optimal parameters

      bit_table_.resize(table_size_ / bits_per_char, static_cast<unsigned char>(0x00));
      //resize the bit table with 0 according to the 
      
      // std::cout<< "salt_count_" << salt_count_ << std::endl;
      // std::cout<< "table_size_" << table_size_ << std::endl;
      // std::cout<< "salt[1]" << salt_[1] << std::endl;
   }

   inline void add_projected_element_count(int i)
   {
      projected_element_count_ += i;
   }

   inline void clear()
   {
       std::fill(bit_table_.begin(), bit_table_.end(), static_cast<unsigned char>(0x00));
       inserted_element_count_ = 0;
   }

   inline void insert(element_type element)
   {
      std::size_t bit_index = 0;
      std::size_t bit = 0;

      for (std::size_t i = 0; i < salt_.size(); ++i)
      {
      	// std::cout << "starting insert" << std::endl;
        compute_indices(hash_ap(element, salt_[i]), bit_index, bit);
		// feed element to hash function, map to a position, present by bit_index and bit
		
        bit_table_[bit_index / bits_per_char] |= bit_mask[bit];
        //set the [bit] of [bit_index / bits_per_char] to 1
        //bit_mask 是准备好的设置点，先取出在vector<char>中的对应位置，再将对应的bit修改为1 
      }
      ++inserted_element_count_; 
	}
	
	//insert a list(array) 
	inline void list_insert(element_type* element_list, unsigned int length){
		for (std::size_t i = 0; i < length; ++i)
      	{
        	insert(element_list[i]);
      	}

	}
	
	//query function
   inline virtual bool contains(element_type element) const
   {
      std::size_t bit_index = 0;
      std::size_t bit = 0;
      
      for (std::size_t i = 0; i < salt_.size(); ++i)
      {
         compute_indices(hash_ap(element, salt_[i]), bit_index, bit);
		 // feed element to hash function, map to a position, present by bit_index and bit
		 
         if ((bit_table_[bit_index / bits_per_char] & bit_mask[bit]) != bit_mask[bit])
         {
            return false;
         }
         //check the whether the position is 1
      }

      return true;
   }
	
	//if u want to know something of the filter
   inline virtual unsigned long long int size() const
   {
      return table_size_;
   }

   inline unsigned long long int element_count() const
   {
      return inserted_element_count_;
   }

   inline std::size_t hash_count()
   {
      return salt_.size();
   }
	
protected:
	//struct optimal_parameters_t
	//其实不需要建构类应该完全可以的 
	struct optimal_parameters_t
   {
      optimal_parameters_t()
      : number_of_hashes(0),
        table_size(0)
      {}

      unsigned int number_of_hashes;
      unsigned long long int table_size;
   };

   optimal_parameters_t optimal_parameters;

   virtual bool compute_optimal_parameters()
   {
      //迭代使存储m最小 
	  /*
	  double min_m  = std::numeric_limits<double>::infinity();
      double min_k  = 0.0;
      double k      = 1.0;
      while (k < 1000.0)
      {
         const double numerator   = (- k * projected_element_count_);
         const double denominator = std::log(1.0 - std::pow(desired_false_positive_probability_, 1.0 / k));
         const double curr_m = numerator / denominator;

         if (curr_m < min_m)
         {
            min_m = curr_m;
            min_k = k;
         }

         k += 1.0;
      }
      optimal_parameters_t& optp = optimal_parameters;
      optp.number_of_hashes = static_cast<unsigned int>(min_k);
      optp.table_size = static_cast<unsigned long long int>(min_m);
	  */ 
     // 使fpp最小的k
	  double ln_2 = std::log(2);
	  double ln_fpp = std::log(desired_false_positive_probability_);
	  double k = -  ln_fpp / ln_2;
	  double m = - (projected_element_count_ * ln_fpp) / (ln_2 * ln_2);
	  optimal_parameters_t& optp = optimal_parameters;
	  //根据公式计算 
      optp.number_of_hashes = static_cast<unsigned int>(k);
      optp.table_size = static_cast<unsigned long long int>(m);
      //取整 
      optp.table_size += (((optp.table_size % bits_per_char) != 0) ? (bits_per_char - (optp.table_size % bits_per_char)) : 0);
	  //将table size 补为 sizeof(char)的倍数 
		
	//check if it is out of range (maybe can throw a warning?) 
      if (optp.number_of_hashes < minimum_number_of_hashes)
         optp.number_of_hashes = minimum_number_of_hashes;
      else if (optp.number_of_hashes > maximum_number_of_hashes)
         optp.number_of_hashes = maximum_number_of_hashes;

      if (optp.table_size < minimum_size)
         optp.table_size = minimum_size;
      else if (optp.table_size > maximum_size)
         optp.table_size = maximum_size;

      return true;
   }

   inline virtual void compute_indices(const bloom_type& hash, std::size_t& bit_index, std::size_t& bit) const
   {
      bit_index = hash % table_size_;
      bit       = bit_index % bits_per_char;
      //std::cout << "bit" << bit << std::endl;
   }
	
	//copy & paste的orz	 
   void generate_unique_salt()
   {
   	  //预定义128个salt
      const unsigned int predef_salt_count = 128;
      static const bloom_type predef_salt[predef_salt_count] =
                                 {
                                    0xAAAAAAAA, 0x55555555, 0x33333333, 0xCCCCCCCC,
                                    0x66666666, 0x99999999, 0xB5B5B5B5, 0x4B4B4B4B,
                                    0xAA55AA55, 0x55335533, 0x33CC33CC, 0xCC66CC66,
                                    0x66996699, 0x99B599B5, 0xB54BB54B, 0x4BAA4BAA,
                                    0xAA33AA33, 0x55CC55CC, 0x33663366, 0xCC99CC99,
                                    0x66B566B5, 0x994B994B, 0xB5AAB5AA, 0xAAAAAA33,
                                    0x555555CC, 0x33333366, 0xCCCCCC99, 0x666666B5,
                                    0x9999994B, 0xB5B5B5AA, 0xFFFFFFFF, 0xFFFF0000,
                                    0xB823D5EB, 0xC1191CDF, 0xF623AEB3, 0xDB58499F,
                                    0xC8D42E70, 0xB173F616, 0xA91A5967, 0xDA427D63,
                                    0xB1E8A2EA, 0xF6C0D155, 0x4909FEA3, 0xA68CC6A7,
                                    0xC395E782, 0xA26057EB, 0x0CD5DA28, 0x467C5492,
                                    0xF15E6982, 0x61C6FAD3, 0x9615E352, 0x6E9E355A,
                                    0x689B563E, 0x0C9831A8, 0x6753C18B, 0xA622689B,
                                    0x8CA63C47, 0x42CC2884, 0x8E89919B, 0x6EDBD7D3,
                                    0x15B6796C, 0x1D6FDFE4, 0x63FF9092, 0xE7401432,
                                    0xEFFE9412, 0xAEAEDF79, 0x9F245A31, 0x83C136FC,
                                    0xC3DA4A8C, 0xA5112C8C, 0x5271F491, 0x9A948DAB,
                                    0xCEE59A8D, 0xB5F525AB, 0x59D13217, 0x24E7C331,
                                    0x697C2103, 0x84B0A460, 0x86156DA9, 0xAEF2AC68,
                                    0x23243DA5, 0x3F649643, 0x5FA495A8, 0x67710DF8,
                                    0x9A6C499E, 0xDCFB0227, 0x46A43433, 0x1832B07A,
                                    0xC46AFF3C, 0xB9C8FFF0, 0xC9500467, 0x34431BDF,
                                    0xB652432B, 0xE367F12B, 0x427F4C1B, 0x224C006E,
                                    0x2E7E5A89, 0x96F99AA5, 0x0BEB452A, 0x2FD87C39,
                                    0x74B2E1FB, 0x222EFD24, 0xF357F60C, 0x440FCB1E,
                                    0x8BBE030F, 0x6704DC29, 0x1144D12F, 0x948B1355,
                                    0x6D8FD7E9, 0x1C11A014, 0xADD1592F, 0xFB3C712E,
                                    0xFC77642F, 0xF9C4CE8C, 0x31312FB9, 0x08B0DD79,
                                    0x318FA6E7, 0xC040D23D, 0xC0589AA7, 0x0CA5C075,
                                    0xF874B172, 0x0CF914D5, 0x784D3280, 0x4E8CFEBC,
                                    0xC569F575, 0xCDB2A091, 0x2CC016B4, 0x5C5F4421
                                 };
		
	//if k <= 预定义的size 
      if (salt_count_ <= predef_salt_count)
      {
         std::copy(predef_salt,
                   predef_salt + salt_count_,
                   std::back_inserter(salt_));

         for (std::size_t i = 0; i < salt_.size(); ++i)
         {
            salt_[i] = salt_[i] * salt_[(i + 3) % salt_.size()] + static_cast<bloom_type>(random_seed_);
         }
      }
    // else，但在大部分场景下应该用不到 
      else
      {
         std::copy(predef_salt, predef_salt + predef_salt_count, std::back_inserter(salt_));

         srand(static_cast<unsigned int>(random_seed_));

         while (salt_.size() < salt_count_)
         {
            bloom_type current_salt = static_cast<bloom_type>(rand()) * static_cast<bloom_type>(rand());
			//随机生成新的salt 
            if (0 == current_salt)
               continue;
			//非0，非重复 
            if (salt_.end() == std::find(salt_.begin(), salt_.end(), current_salt))
            {
               salt_.push_back(current_salt);
            }
         }
      }
   }

	//hash函数 
   inline bloom_type hash_ap(element_type element, bloom_type hash) const
   {
   		// std::cout << "enter" << std::endl;
    	const unsigned int& first = element.first;
        const unsigned int& second = element.second;
		// std::cout << "i2:" << i2 << std::endl;
		
		//也是copy paste的 
    	hash ^= (hash <<  7) ^  first * (hash >> 3) ^
             (~((hash << 11) + (second ^ (hash >> 5))));
		
      	// std::cout << "hash" << hash << std::endl;
      	return hash;
   }

   std::vector<bloom_type>    salt_;
   std::vector<unsigned char> bit_table_;
   unsigned int               salt_count_;
   unsigned long long int     table_size_;
   unsigned long long int     projected_element_count_;
   double     desired_false_positive_probability_;
   unsigned long long int     inserted_element_count_;
   unsigned long long int     random_seed_;
   
};

#endif
