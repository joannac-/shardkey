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

    def __init__(self, data, infile):
        self.infile = open(str(infile), "r")
        self.number_shards = len(data)
        self.chunk_size = 64

        self.data = data

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

            ax.set_ylim(-max_chunks, 0)
            ax.set_xlim(0, 10)
            ax.axes.get_xaxis().set_visible(False)
            ax.axes.get_yaxis().set_visible(False)

            #self.artists[0].set_color((0.4, 0.2, 0.4))


        # next button
        axes = plt.axes([0.8, 0.025, 0.1, 0.04])
        self.button_next = plt.Button(axes, 'Next')
        self.button_next.on_clicked(self._click_next)

        # randomize button
#        axes = plt.axes([0.025, 0.025, 0.1, 0.04])
#        self.button_randomize = plt.Button(axes, 'Randomize')
#        self.button_randomize.on_clicked(self._click_randomize)

        plt.gcf().canvas.draw()


    def _click_next(self, event):
        line = self.infile.readline()
        if not line:
            print "No more lines!"
            return
        toks = line.split()
        sh = int(toks[1])
        cl = int(toks[2])
        if toks[0] == "add":
            self.data[sh][cl].incr();
            print "add"
        elif toks[0] == "split":
            currsize = self.data[sh][cl].getsize()
            nsize = int(toks[3])
            self.data[sh][cl].setsize(nsize)
            self.data[sh].append(Chunk(currsize-nsize))
            print "split"
        elif toks[0] == "move":
            news = int(toks[3])
            chunk = self.data[sh].pop(cl)
            self.data[news].append(chunk)
            print "move"
            
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
    
    script, infile, numshards = argv
    # create data
    #data = [ [Chunk(4), Chunk(7), Chunk(12), Chunk(3), Chunk(8)], [Chunk(6), Chunk(3)] ]
    data = []
    for i in xrange(int(numshards)):
        data.append([])

    data[0].append(Chunk(0))

    shardvis = ShardVis(data, infile)
    plt.show()






