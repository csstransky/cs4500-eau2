/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
public:  
  bool* vals_;  // owned; data
  size_t size_; // number of elements
  size_t num_tagged_;

  /** Creates a set of the same size as the dataframe. */ 
  Set(DataFrame* df) : Set(df->nrows()) {}

  /** Creates a set of the given size. */
  Set(size_t sz) :  vals_(new bool[sz]), size_(sz) {
    num_tagged_ = 0;
    for(size_t i = 0; i < size_; i++)
      vals_[i] = false; 
  }

  ~Set() { delete[] vals_; }

  /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
   *  values can occur if there are references to pids or uids in commits
   *  that did not appear in projects or users.
   */
  void set(size_t idx) {
    if (idx >= size_ ) return; // ignoring out of bound writes
    if (!vals_[idx]) {
      vals_[idx] = true; 
      num_tagged_++;
    }
          
  }

  /** Is idx in the set?  See comment for set(). */
  bool test(size_t idx) {
    if (idx >= size_) return false; // ignoring out of bound reads
    return vals_[idx];
  }

  size_t size() { return size_; }

  size_t tagged() { return num_tagged_; }

  /** Performs set union in place. */
  void union_(Set& from) {
    for (size_t i = 0; i < from.size_; i++) 
      if (from.test(i))
	      set(i);
  }

  void print() {
    for (size_t i = 0; i < size_; i++) {
      if(vals_[i]) printf("%zu ", i);
    }
    printf("\n");
  }
};


/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Rower {
public:
  Set& set_; // set to update
  
  SetUpdater(Set& set): set_(set) {}

  /** Assume a row with at least one column of type I. Assumes that there
   * are no missing. Reads the value and sets the corresponding position.
   * The return value is irrelevant here. */
  bool accept(Row & row) { set_.set(row.get_int(0));  return false; }

  void join_delete(Rower* other) { delete other; }

};

/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * dataframe. The data contains all the values in the set. The dataframe has
 * at least one integer column.
 ****************************************************************************/
class SetWriter: public Rower {
public:
  Set& set_; // set to read from
  int i_ = 0;  // position in set

  SetWriter(Set& set): set_(set) { }

  bool accept(Row & row) { 
    /** Skip over false values and stop when the entire set has been seen */
    while (i_ < set_.size() && set_.test(i_) == false) ++i_;
    
    if (i_ == set_.size()) {
      return true;
    } else {
      row.set(0, i_++);
      return false;
    }		
  }

  void join_delete(Rower* other) { delete other; }
};

/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Rower {
public:
  Set& uSet; // set of collaborator 
  Set& pSet; // set of projects of collaborators
  Set newProjects;  // newly tagged collaborator projects

  ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
    uSet(uSet), pSet(pSet), newProjects(proj) {}

  /** The data frame must have at least two integer columns. The newProject
   * set keeps track of projects that were newly tagged (they will have to
   * be communicated to other nodes). */
  bool accept(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (uSet.test(uid)) 
      if (!pSet.test(pid)) {
    	  pSet.set(pid);
        newProjects.set(pid);
      }
    return false;
  }

	void join_delete(Rower* other) { delete other; }
};

/***************************************************************************
 * The UserTagger is a reader that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer. 
 *************************************************************************/
class UsersTagger : public Rower {
public:
  Set& pSet;
  Set& uSet;
  Set newUsers;

  UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
    pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }

  bool accept(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (pSet.test(pid)) 
      if(!uSet.test(uid)) {
				uSet.set(uid);
				newUsers.set(uid);
      }
    return false;
  }

	void join_delete(Rower* other) { delete other; }
};