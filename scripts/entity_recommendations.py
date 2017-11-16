
import cPickle
import sys
from pprint import pprint

if __name__=="__main__":
    entity_type = raw_input('What type of entities would you like recommendations for?\n')
    try:
        filename = '%s_recommender.pkl' % entity_type
        with open(filename) as f:
            recommender = cPickle.load(f)
    except IOError:
        print 'Sorry, a file called %s doesn\'t exist in this directory. \
            Try running the recommendation_builder script again.'

    while True:
        entity_name = raw_input('What\'s your favorite %s?\n' % entity_type).lower()
        if entity_name == 'exit':
            sys.exit()
        try:
            print "You might like:"
            pprint(recommender[entity_name])
        except KeyError:
            print "Oops, didn't find anything for that %s!" % entity_type
