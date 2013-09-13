from matplotlib import pyplot as plt
from numpy import random
import numpy as np
from sys import argv

class Chunk(object):

    def __init__(self, size, minkey=None, maxkey=None):
        self.size = size
        self.minkey = minkey
        self.maxkey = maxkey
    
    def incr(self):
        self.size += 1

    def setsize(self, s):
        self.size = s

    def getsize(self):
        return self.size 


class ShardVis(object):

    def __init__(self, infile):
        self.infile = open(str(infile), "r").readlines()
        stats = self.infile.pop()
        stattoks = stats.split()
        self.number_shards = int(stattoks[0])
        self.chunk_size = int(stattoks[1])
        self.updateds = -1
        self.updatedc = []
        self.colormap = plt.get_cmap("Reds")

        self.data = []
        for i in xrange(self.number_shards):
            self.data.append([])
        self.data[0].append(Chunk(0))

        self.doccount = []
        for i in xrange(self.number_shards):
            self.doccount.append(0)

        self.writecount = []
        for i in xrange(self.number_shards):
            self.writecount.append(0)
        self.writecountall = 0

        self._render()


    def _render(self):
        plt.clf()

        max_chunks = max( [len(shard) for shard in self.data] )
        #print max_chunks

        for shard in xrange(self.number_shards):
            ax = plt.subplot(1, self.number_shards, shard+1)

            shard_data = self.data[shard]

            pos = np.arange(0, -len(shard_data), -1) - .5
            self.artists = plt.barh(pos, [ch.size for ch in shard_data], align='center')

            for artist in self.artists:
                if artist.get_width() > self.chunk_size:
                    artist.set_color((1.0, 0.0, 0.0, 0.9))

            x = max(max_chunks, 5)
            ax.set_ylim(-1*x, 0)
            ax.set_xlim(0, self.chunk_size)
            ax.axes.get_xaxis().set_visible(False)
            ax.axes.get_yaxis().set_visible(False)

            if self.writecountall > 0:
                col = self.colormap(1.0*self.writecount[shard] / self.writecountall)
                ax.patch.set_facecolor(col[:3])
                ax.patch.set_alpha(col[3])
            else:
                ax.patch.set_facecolor((1,1,1))
                ax.patch.set_alpha(1)
    
            if shard == self.updateds:
                for c in self.updatedc:
                    self.artists[c].set_color((0.0, 1.0, 0.0, 0.9))
                    
            
            xcoord = shard/self.number_shards + 0.25
            plt.text(xcoord, 0.195, '#docs = ' + str(self.doccount[shard]))



        # next button
        axes = plt.axes([0.9, 0.025, 0.08, 0.04])
        self.button_next = plt.Button(axes, 'Next')
        self.button_next.on_clicked(self._click_next)


        # randomize button
#        axes = plt.axes([0.025, 0.025, 0.1, 0.04])
#        self.button_randomize = plt.Button(axes, 'Randomize')
#        self.button_randomize.on_clicked(self._click_randomize)

        plt.gcf().canvas.draw()


    def _click_next(self, event):
        for i in xrange(10):
            line = self.infile.pop(0)
            if not line:
                print "No more lines!"
                return
            toks = line.split()
            sh = int(toks[1])
            cl = int(toks[2])
            if toks[0] == "add":
                self.data[sh][cl].incr();
                self.doccount[sh] += 1
                self.writecount[sh] += 1
                self.writecountall += 1
                self.updateds = sh
                self.updatedc = [cl]
                #print "add"
            elif toks[0] == "split":
                currsize = self.data[sh][cl].getsize()
                nsize = int(toks[3])
                self.data[sh][cl].setsize(nsize)
                self.data[sh].append(Chunk(currsize-nsize))
                self.updateds = sh
                self.updatedc = [cl]
                n = len(self.data[sh])
                self.updatedc.append(n-1)
                self.writecount[sh] += 1
                self.writecountall += 1
                #print "split"
            elif toks[0] == "move":
                news = int(toks[3])
                chunk = self.data[sh].pop(cl)
                self.data[news].append(chunk)
                self.doccount[sh] -= chunk.getsize()
                self.doccount[news] += chunk.getsize()
                self.writecount[news] += 1
                self.writecountall += 1
                self.updateds = news
                n = len(self.data[news])
                self.updatedc = [n-1]
                #print "move"
            self._render()
        
#        self.number_bars += 1
#        self.data = random.random(self.number_bars)
#        self._render()


#    def _click_randomize(self, event):
#        self.data = random.random(self.number_bars)

#        for data, artist in zip(self.data, self.artists):
#            artist.set_width(data)

#        plt.gcf().canvas.draw()     


if __name__ == '__main__':
    
    script, infile = argv
    # create data
    #data = [ [Chunk(4), Chunk(7), Chunk(12), Chunk(3), Chunk(8)], [Chunk(6), Chunk(3)] ]

    shardvis = ShardVis(infile)
    plt.show()






