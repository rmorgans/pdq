from os.path import basename

try:
    import apsw
except ImportError as exc:
    sys.stderr.write("Error: failed to import apsw module ({})".format(exc))

class pdq(object):
    """ Priority disk queue, based on sqlite.
    """
    # TODO add multiple put()
    # fix iterators
    # consume()
    # an index

    def __init__(self,filename):
        """ Initialise sqlite database with filename
        """
        self.pdq=apsw.Connection(filename)
        self.pdq.setbusytimeout(2000)
        self.filename=self.pdq.filename
        self.name=basename(self.filename)
        pdq_table=self.pdq.cursor().execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pdq'").fetchall()
        if not pdq_table:
            self._create()

    def _create(self):
        """ Local function to initalise database
        """
        with self.pdq:
            c=self.pdq.cursor()            
            c.execute('CREATE TABLE pdq (item blob,priority int)')
            c.execute('CREATE INDEX priority_index ON pdq (priority)')

    def _toiter(self,item):
        if not hasattr(item,'__iter__'):
            item=[item]
        return item

    def put(self,items,priority=0):
        """ Put item(s) on the queue with optional priority
        """
        with self.pdq:
            self.pdq.cursor().executemany('insert into pdq values (?,?)',[(item,priority) for item in self._toiter(items)])

    def get(self,number=1):
        """ get list of item(s) from the queue based on priority
        """
        with self.pdq:
            c=self.pdq.cursor()
            l=list(c.execute("select rowid,item from pdq order by priority desc limit ?",(number,)))
            c.executemany("delete from pdq where rowid = ?",[(rowid,) for rowid,item in l])
            return [link for rowid,link in l]

    def count(self):
        """ count item(s) on the queue
        """
        with self.pdq:
            (count,)=self.pdq.cursor().execute('select count(*) from pdq').next()
            return count

    def vacuum(self):
        """ count item(s) on the queue
        """
        self.pdq.cursor().execute('vacuum')

    def clear(self):
        """ count item(s) on the queue
        """
        self.pdq.cursor().execute('drop table pdq')
        self._create()
