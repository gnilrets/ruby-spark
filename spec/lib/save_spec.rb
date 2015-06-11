require 'spec_helper'

RSpec.describe 'Spark::RDD' do

  context '.save_as_text_file' do
    let(:file)    { File.join('spec', 'inputs', 'numbers_0_100.txt') }

    def serializer
      Spark::Serializer.build { __batched__(__marshal__, 1) }
    end

    def file_rdd
      $sc.text_file(file, 2, Encoding::UTF_8, serializer)
    end

    def par_rdd
      $sc.parallelize( (0..5).collect { |i| i.to_s }, 2, serializer)
    end

    it 'saves the file_rdd' do
      Dir.mktmpdir do |tmpdir|
        file_rdd.save_as_text_file(File.join(tmpdir,'file_rdd.txt'))
        result = Dir.glob(File.join(tmpdir,'file_rdd.txt','*')).collect { |file| File.readlines(file).collect { |l| l.chomp } }.flatten

        expect(result).to eq (0..100).collect { |i| i.to_s }
      end
    end

    it 'saves the par_rdd' do
      Dir.mktmpdir do |tmpdir|
        par_rdd.save_as_text_file(File.join(tmpdir,'par_rdd.txt'))
        result = Dir.glob(File.join(tmpdir,'par_rdd.txt','*')).collect { |file| File.readlines(file).collect { |l| l.chomp } }.flatten

        expect(result).to eq (0..5).collect { |i| i.to_s }
      end
    end

  end
end
