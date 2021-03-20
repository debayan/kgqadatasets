import tensorflow as tf
import numpy as np
import pointer_net 
import time
import os
import json
import sys

tf.app.flags.DEFINE_integer("batch_size", 20,"Batch size.")
tf.app.flags.DEFINE_integer("max_input_sequence_len", 100, "Maximum input sequence length.")
tf.app.flags.DEFINE_integer("max_output_sequence_len", 100, "Maximum output sequence length.")
tf.app.flags.DEFINE_integer("rnn_size", 32, "RNN unit size.")
tf.app.flags.DEFINE_integer("attention_size", 100, "Attention size.")
tf.app.flags.DEFINE_integer("num_layers", 1, "Number of layers.")
tf.app.flags.DEFINE_integer("beam_width", 5, "Width of beam search .")
tf.app.flags.DEFINE_float("learning_rate", 0.001, "Learning rate.")
tf.app.flags.DEFINE_float("max_gradient_norm", 5.0, "Maximum gradient norm.")
tf.app.flags.DEFINE_boolean("forward_only", False, "Forward Only.")
#tf.app.flags.DEFINE_string("log_dir", "./log", "Log directory")
tf.app.flags.DEFINE_string("data_path", "./train.txt", "Data path.")
tf.app.flags.DEFINE_string("test_data_path", "./test.txt", "Data path.")
tf.app.flags.DEFINE_integer("steps_per_checkpoint", 200, "frequence to do per checkpoint.")

FLAGS = tf.app.flags.FLAGS
FLAGS.log_dir='./'

class ConvexHull(object):
  def __init__(self):
    self.testgraph = tf.Graph()
    with self.testgraph.as_default():
      config = tf.ConfigProto()
      config.gpu_options.allow_growth = True
      config.operation_timeout_in_ms=6000
      self.testsess = tf.Session(config=config)
    self.build_model()
    #self.read_data()
    #self.test_read_data()
    self.id2word = json.loads(open('id2word.txt').read())
    self.word2id = json.loads(open('word2id.txt').read())
    

  def vectorise(self,tokenids):
    print("tokenids: ",tokenids)
    inputs = []
    enc_input_weights = []
    enc_input = []
    for t in tokenids:
      enc_input.append(int(t))
    enc_input_len = len(enc_input)
    enc_input += [0]*((FLAGS.max_input_sequence_len-enc_input_len))
    enc_input = np.array(enc_input).reshape([-1,1])
    inputs.append(enc_input)
    weight = np.zeros(FLAGS.max_input_sequence_len)
    weight[:enc_input_len]=1
    enc_input_weights.append(weight)
    self.test_inputs = np.stack(inputs)
    self.test_enc_input_weights = np.stack(enc_input_weights)
    print("Load inputs:            " +str(self.test_inputs.shape))
    print("Load enc_input_weights: " +str(self.test_enc_input_weights.shape))

  def build_model(self):
    with self.testgraph.as_default():
      self.testmodel = pointer_net.PointerNet(batch_size=1,
                    max_input_sequence_len=FLAGS.max_input_sequence_len,
                    max_output_sequence_len=FLAGS.max_output_sequence_len,
                    rnn_size=FLAGS.rnn_size,
                    attention_size=FLAGS.attention_size,
                    num_layers=FLAGS.num_layers,
                    beam_width=FLAGS.beam_width,
                    learning_rate=FLAGS.learning_rate,
                    max_gradient_norm=FLAGS.max_gradient_norm,
                    forward_only=True)

      # Prepare Summary writer
      # Try to get checkpoint
      ckpt = tf.train.get_checkpoint_state(FLAGS.log_dir)
      print("Load model parameters from %s" % ckpt.model_checkpoint_path)
      self.testmodel.saver.restore(self.testsess, ckpt.model_checkpoint_path)


  def eval(self):
    """ Randomly get a batch of data and output predictions """  
    #inputs,enc_input_weights, outputs, dec_input_weights = self.get_test_batch()
    count = 0
    ans = None
    for input_,enc_input_weights_ in zip(self.test_inputs,self.test_enc_input_weights):
      try:
        predicted_ids = self.testmodel.step(self.testsess, [input_], [enc_input_weights_])
      except Exception as err:
        print(err)
        continue
      count += 1
      print("="*20)
      #print("inputs: ",inputs[i])
      #query = []
      #for x in input_:
      #  if x[0] == -1:
      #    break
      #  query.append(self.id2word[str(x[0])])   
      #print("input query: ", ' '.join(query))
      #print("* %dth sample target: %s" % (i,str(outputs[i,:]-2)))
      beams = []
      for predict in predicted_ids[0]:
        print("predict:",predict)
        predquery = []
        for x in predict:
          if x > 0:
            predquery.append(self.id2word[str(input_[x-1][0])])
        ans = ' '.join(predquery)
        beams.append(ans)
        print("prediction: ", ans)
    return beams

  def correct(self,query):
    tokens = query.split(' ')
    tokenids = [self.word2id[x] for x in tokens]
    tokenids.append(-1)
    for x in self.id2word.keys():
      if int(x) in tokenids:
        continue
      else:
        tokenids.append(int(x))
    self.vectorise(tokenids)
    ans = self.eval()
    return ans

if __name__ == "__main__":
  c = ConvexHull()
  inputquery = 'select distinct ?vr0 where { <entpos@@1> <predpos@@1> ?vr0 }' 
  beams = c.correct(inputquery)
  print("input: ", inputquery)
  for ans in beams:
    print("corec: ",ans)
