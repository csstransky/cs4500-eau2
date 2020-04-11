#include "../dataframe/dataframe.h"
#include "application.h"
#include "linus_rowers.h"
#include "arguments.h"


/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
  int DEGREES = 4;  // How many degrees of separation form linus?
  int LINUS = 4967;   // The uid of Linus (offset in the user df)
  const char* PROJ = "data/projects.ltgt";
  const char* USER = "data/users.ltgt";
  const char* COMM = "data/commits.ltgt";
  DataFrame* projects; //  pid x project name
  DataFrame* users;  // uid x user name
  DataFrame* commits;  // pid x uid x uid 
  Set* uSet; // Linus' collaborators
  Set* pSet; // projects of collaborators

  Linus(size_t node_index, const char* my_ip, const char* server_ip) : Application(node_index, my_ip, server_ip) {}

  ~Linus() {
    delete projects;
    delete users;
    delete commits;
    delete uSet;
    delete pSet;
  }

  /** Compute DEGREES of Linus.  */
  void run_() override {
    readInput();
    for (size_t i = 0; i < DEGREES; i++) step(i);
  }

  Key* mk_key(const char* name, size_t stage, size_t index) {
    String s(name);
    s.concat('-');
    s.concat(stage);
    s.concat('-');
    s.concat(index);
    return new Key(&s, index);
 }

  /** Node 0 reads three files, cointainng projects, users and commits, and
   *  creates thre dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with a the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. **/
  void readInput() {
    Key pK("projs", 0);
    Key uK("usrs", 0);
    Key cK("comts", 0);
    if (node_index_ == 0) {
      pln("Reading...");
      projects = DataFrame::from_file(&pK, &kd_, const_cast<char*>(PROJ));
      p("    ").p(projects->nrows()).pln(" projects");
      users = DataFrame::from_file(&uK, &kd_, const_cast<char*>(USER));
      p("    ").p(users->nrows()).pln(" users");
      commits = DataFrame::from_file(&cK, &kd_, const_cast<char*>(COMM));
       p("    ").p(commits->nrows()).pln(" commits");
       // This dataframe contains the id of Linus.
       Key* user = mk_key("users", 0, 0);
       delete DataFrame::from_scalar(user, &kd_, LINUS);
       delete user;
    } else {
       projects = kd_.wait_and_get(&pK);
       users = kd_.wait_and_get(&uK);
       commits = kd_.wait_and_get(&cK);
    }
    uSet = new Set(users);
    pSet = new Set(projects);
 }

 /** Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. */
  void step(size_t stage) {
    p("Stage ").pln(stage);
    // Key of the shape: users-stage-0
    Key* uK = mk_key("users", stage, 0);
    // A df with all the users added on the previous round
    DataFrame* newUsers = kd_.wait_and_get(uK);  
    delete uK;  
    Set delta(users);
    SetUpdater upd(delta);  
    newUsers->map(upd); // all of the new users are copied to delta.
    delete newUsers;
    ProjectsTagger ptagger(delta, *pSet, projects);
    commits->local_map(ptagger); // marking all projects touched by delta
    merge(ptagger.newProjects, "projects", stage);
    pSet->union_(ptagger.newProjects); // 
    UsersTagger utagger(ptagger.newProjects, *uSet, users);
    commits->local_map(utagger);
    merge(utagger.newUsers, "users", stage + 1);
    uSet->union_(utagger.newUsers);
    p("    after stage ").p(stage).pln(":");
    p("        tagged projects: ").pln(pSet->tagged());
    p("        tagged users: ").pln(uSet->tagged());
  }

  /** Gather updates to the given set from all the nodes in the systems.
   * The union of those updates is then published as dataframe.  The key
   * used for the otuput is of the form "name-stage-0" where name is either
   * 'users' or 'projects', stage is the degree of separation being
   * computed.
   */ 
  void merge(Set& set, char const* name, int stage) {
    if (node_index_ == 0) {
      for (size_t i = 1; i < kd_.get_kv()->get_num_other_nodes(); ++i) {
        Key* nK = mk_key(name, stage, i);
        DataFrame* delta = dynamic_cast<DataFrame*>(kd_.wait_and_get(nK));
        delete nK;
        p("    received delta of ").p(delta->nrows()).p(" elements from node ").pln(i);
        SetUpdater upd(set);
        delta->map(upd);
        delete delta;
      }
      p("    storing ").p(set.tagged()).pln(" merged elements");
      SetWriter writer(set);
      Key* k = mk_key(name, stage, 0);
      delete DataFrame::from_rower(k, &kd_, "I", writer);
      delete k;
    } else {
      p("    sending ").p(set.tagged()).pln(" elements to master node");
      SetWriter writer(set);
      Key* k = mk_key(name, stage, node_index_);
      delete DataFrame::from_rower(k, &kd_, "I", writer);
      delete k;
      Key* mK = mk_key(name, stage, 0);
      DataFrame* merged = dynamic_cast<DataFrame*>(kd_.wait_and_get(mK));
      delete mK;
      p("    receiving ").p(merged->nrows()).pln(" merged elements");
      SetUpdater upd(set);
      merged->map(upd);
      delete merged;
    }
  }
}; // Linus

int main(int argc, const char** argv) {
  const char* client_ip_address = get_input_client_ip_address(argc, argv);
  const char* server_ip_address = get_input_server_ip_address(argc, argv);
  int node_index = get_input_node_index(argc, argv);
  Linus app(node_index, client_ip_address, server_ip_address);
  app.run_();
}