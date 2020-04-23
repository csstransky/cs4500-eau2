#include <sys/wait.h>

#define TEST

#include "../src/dataframe/dataframe.h"
#include "../src/application/application.h"
#include "../src/application/linus_rowers.h"
#include "../src/networks/rendezvous_server.h"

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
  int DEGREES = 3;  // How many degrees of separation form linus?
  int LINUS = 24;   // The uid of Linus (offset in the user df)
  const char* PROJ = "data/sub_projects.ltgt";
  const char* USER = "data/sub_users.ltgt";
  const char* COMM = "data/sub_commits.ltgt";
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
    for (size_t i = 0; i < DEGREES; i++) {
      step(i);

      if (i == 0) {
        assert(uSet->tagged() == 2);
        for (size_t ii = 0; ii < uSet->size(); ii++) {
          if (ii == 24 || ii == 25) {
            assert(uSet->test(ii) == true);
          } else {
            assert(uSet->test(ii) == false);
          } 
        }

        assert(pSet->tagged() == 2);
        for (size_t ii = 0; ii < pSet->size(); ii++) {
          if (ii == 3 || ii == 4) {
            assert(pSet->test(ii) == true);
          } else {
            assert(pSet->test(ii) == false);
          } 
        }
      }

      if (i == 1) {
        assert(uSet->tagged() == 5);
        for (size_t ii = 0; ii < uSet->size(); ii++) {
          if (ii == 24 || ii == 25 || ii == 2 || ii == 3 || ii == 5) {
            assert(uSet->test(ii) == true);
          } else {
            assert(uSet->test(ii) == false);
          } 
        }

        assert(pSet->tagged() == 3);
        for (size_t ii = 0; ii < pSet->size(); ii++) {
          if (ii == 3 || ii == 4 || ii == 1) {
            assert(pSet->test(ii) == true);
          } else {
            assert(pSet->test(ii) == false);
          } 
        }
      }

      if (i == 2) {
        assert(uSet->tagged() == 6);
        for (size_t ii = 0; ii < uSet->size(); ii++) {
          if (ii == 24 || ii == 25 || ii == 2 || ii == 3 || ii == 5 || ii == 1) {
            assert(uSet->test(ii) == true);
          } else {
            assert(uSet->test(ii) == false);
          } 
        }

        assert(pSet->tagged() == 4);
        for (size_t ii = 0; ii < pSet->size(); ii++) {
          if (ii == 3 || ii == 4 || ii == 1 || ii == 2) {
            assert(pSet->test(ii) == true);
          } else {
            assert(pSet->test(ii) == false);
          } 
        }
      }
    }
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
      projects = DataFrame::from_file(&pK, &kd_, const_cast<char*>(PROJ));
      users = DataFrame::from_file(&uK, &kd_, const_cast<char*>(USER));
      commits = DataFrame::from_file(&cK, &kd_, const_cast<char*>(COMM));
      // This dataframe contains the id of Linus.
      Key* user = mk_key("users", 0, 0);
      delete DataFrame::from_scalar(user, &kd_, LINUS);
      delete user;
    } else {
      projects = kd_.wait_and_get(&pK);
      users = kd_.wait_and_get(&uK);
      commits = kd_.wait_and_get(&cK);
    }
    assert(projects->ncols() == 2);
    assert(projects->nrows() == 300);
    assert(strcmp(projects->get_schema().types_->c_str(), "IS") == 0);
    assert(users->ncols() == 2);
    assert(users->nrows() == 5000);
    assert(strcmp(users->get_schema().types_->c_str(), "IS") == 0);
    assert(commits->ncols() == 3);
    assert(commits->nrows() == 500);
    assert(strcmp(commits->get_schema().types_->c_str(), "III") == 0);
    uSet = new Set(users);
    pSet = new Set(projects);
 }

 /** Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. */
  void step(size_t stage) {
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
        SetUpdater upd(set);
        delta->map(upd);
        delete delta;
      }
      SetWriter writer(set);
      Key* k = mk_key(name, stage, 0);
      delete DataFrame::from_rower(k, &kd_, "I", writer);
      delete k;
    } else {
      SetWriter writer(set);
      Key* k = mk_key(name, stage, node_index_);
      delete DataFrame::from_rower(k, &kd_, "I", writer);
      delete k;
      Key* mK = mk_key(name, stage, 0);
      DataFrame* merged = dynamic_cast<DataFrame*>(kd_.wait_and_get(mK));
      delete mK;
      SetUpdater upd(set);
      merged->map(upd);
      delete merged;
    }
  }
}; // Linus

// author: kaylindevchand & csstransky
void test_linus() {
  int num_nodes = 3;
  int cpid[num_nodes];
  const char* server_ip = "127.0.0.1";
  const char** client_ips = new const char*[num_nodes];
  client_ips[0] = "127.0.0.2";
  client_ips[1] = "127.0.0.3";
  client_ips[2] = "127.0.0.4";

  RServer* server = new RServer(server_ip); 

  for (int i = 0; i < num_nodes; i++) {
    if ((cpid[i] = fork())) {
      // parent, do nothing now
    } else {
      // child process
      Linus* demo = new Linus(i, client_ips[i], server_ip);
      demo->run_();
      delete demo;
      delete server;
      delete[] client_ips;
      exit(0);
    } 
  }

  // In parent process
  server->run_server();
  server->wait_for_shutdown();

  // wait for child to finish
  for (int i = 0; i < num_nodes; i++) {
    int st;
    waitpid(cpid[i], &st, 0);
  }
  delete server;
  delete[] client_ips;

  printf("Linus application test passed!\n");
}

void test_set() {
  size_t count = 500;
  Set set(count);
  assert(set.size() == count);
  
  for (size_t ii = 0; ii < count; ii++) {
    set.set(ii*2);
  }

  for (size_t ii = 0; ii < count; ii += 2) {
    assert(set.test(ii) == true);
    assert(set.test(ii+1) == false);
  }

  for (size_t ii = count; ii < 2*count; ii++) {
    assert(set.test(ii) == false);
  }

  Set set2(2);

  set2.union_(set);
  assert(set2.test(0) == true);
  assert(set2.test(1) == false);

  printf("Set test passed!\n");
}

void test_set_updater() {
  int cpid;
  const char* server_ip = "127.0.0.1";
  const char* client_ip = "127.0.0.2";

  RServer* server = new RServer(server_ip); 

  if ((cpid = fork())) {
    // parent, do nothing now
  } else {
    // child process
    KD_Store* kd = new KD_Store(0, client_ip, server_ip);

    size_t count = 50;

    Set* set = new Set(count);
    set->set(2);
    set->set(4);

    for (size_t ii = 0; ii < count; ii++) {
      if (ii == 2 || ii == 4) {
        assert(set->test(ii) == true);
      } else {
        assert(set->test(ii) == false);
      } 
    }

    SetUpdater* set_updater = new SetUpdater(*set);
    int array[2] = {5, 10};
    Key* key = new Key("key", 0);
    DataFrame* df = DataFrame::from_array(key, kd, 2, array);
    df->map(*set_updater);

    for (size_t ii = 0; ii < count; ii++) {
      if (ii == 2 || ii == 4 || ii == 5 || ii == 10) {
        assert(set->test(ii) == true);
      } else {
        assert(set->test(ii) == false);
      } 
    }

    kd->application_complete();
    delete kd;
    delete set;
    delete set_updater;
    delete key;
    delete df;
    delete server;
    exit(0);
  } 
  

  // In parent process
  server->run_server(10);
  server->wait_for_shutdown();

  // wait for child to finish
  int st;
  waitpid(cpid, &st, 0);

  delete server;

  printf("Set updater test passed!\n");
}

void test_set_writer() {
  int cpid;
  const char* server_ip = "127.0.0.1";
  const char* client_ip = "127.0.0.2";

  RServer* server = new RServer(server_ip); 

  if ((cpid = fork())) {
    // parent, do nothing now
  } else {
    // child process
    KD_Store* kd = new KD_Store(0, client_ip, server_ip);

    size_t count = 50;

    Set* set = new Set(count);

    SetWriter* set_writer = new SetWriter(*set);
    Key* key = new Key("key", 0);
    DataFrame* df = DataFrame::from_rower(key, kd, "I", *set_writer);

    assert(set->tagged() == 0);
    assert(set->size() == count);
    for (size_t ii = 0; ii < count; ii++) {
      assert(set->test(ii) == false);
    }

    assert(df->nrows() == 0);
    assert(df->ncols() == 1);

    kd->application_complete();
    delete kd;
    delete set;
    delete set_writer;
    delete key;
    delete df;
    delete server;
    exit(0);
  } 
  

  // In parent process
  server->run_server(10);
  server->wait_for_shutdown();

  // wait for child to finish
  int st;
  waitpid(cpid, &st, 0);

  delete server;

  printf("Set writer test passed!\n");
}

int main(int argc, char** argv) {
  test_set();
  test_set_updater();
  test_set_writer();
  test_linus();
}