
import pickle
import sys

if __name__=="__main__":
    with open('artist_model.pkl') as f:
        model = pickle.load(f)

    with open('artist_plays.pkl') as f:
        plays = pickle.load(f)

    while True:
        user_id = raw_input('What\'s your user_id?\n').lower()
        if user_id == 'exit':
            sys.exit()
        try:
            user_items = plays.T.tocsr()
            recommendations = model.recommend(user_id, user_items)
            print "You might like: %s" % recommendations
        except KeyError:
            print "Oops, didn't find anything for that user!"
