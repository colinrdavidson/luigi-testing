# minimal Luigi Example - Gouthaman Balaraman
# http://gouthamanbalaraman.com/blog/building-luigi-task-pipeline.html

import luigi

    
class SimpleTask(luigi.Task):
    """
    This simple task prints Hello World!
    """

    def output(self):
        return luigi.LocalTarget("output.txt")
 
    def run(self):
        _out = self.output().open('w')
        _out.write(u"Hello World!\n")
        _out.close()
 

if __name__ == '__main__':
    from luigi.mock import MockFile # import this here for compatibility with Windows
    # if you are running windows, you wouldn need --lock-pid-dir argument; modified run would look like
    # luigi.run(["--lock-pid-dir", "D:\\temp\\", "--local-scheduler"], main_task_cls=SimpleTask)
    luigi.run(["--local-scheduler"], main_task_cls=SimpleTask)
