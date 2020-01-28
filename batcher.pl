#!/usr/bin/perl

use strict;
use Text::Glob qw(match_glob);

open(my $batches_file, $ARGV[0]) or die $!;
my $batch_reviewers = {};
my $found_batches = {};
while (<$batches_file>) {
    next if /^#/;
    s/\r?\n//;
    my @fields = split(/\t/);
    $fields[0],"\n";
    $batch_reviewers->{$fields[0]}=$fields[2];
}
open(my $files_file, "logging_cpp_files.txt") or die $!;
while (<$files_file>) {
    next if /^#/;
    s/\r?\n//;
    my $match = "";
    #print("FILE $_\n");
    foreach my $key (sort { length($b) <=> length($a) } keys %$batch_reviewers) {
        my $regex;
        if ($key =~ /\*/) {
            $regex = $key;
            if ($key =~ /\//) {
                $regex =~ s[\*][\[^\/\]\*]g;
            } else {
                $regex =~ s[\*][\.\*]g;
            }
            $regex = qr($regex);
        } else {
            $regex = "$key/.*";
        }
        #print ("REGEX: $regex\n");
        if (m[$regex]) {
            #print("MATCH!!\n");
            $match = $key;
            last;
        }
    }
    if ($match) {
        push(@{$found_batches->{$match}},$_);
    } else {
        print("?\tunknown\t$_\n");
    }
}

sub run {
    my @cmd = @_;
    print( join(" ",map { / /?"\"$_\"":$_ } @cmd),"\n" );
    #exec(@cmd);
}

for my $batch (sort keys %$found_batches) {
    my @files = @{$found_batches->{$batch}};
    print ("BATCH $batch $batch_reviewers->{$batch}\n");
    run(qw(git add logging_cpp_files.txt batcher.pl logv1tologv2 run.sh));
    run(qw(git commit -m xxx));
    run(qw(git cifa));
    run("./logv1tologv2",@files); 
    run(qw(buildscripts/clang_format.py format));
    run(qw(evergreen patch -p mongodb-mongo-master  --yes -a required -f), "-d", "structured logging auto-conversion of $batch");
    run(qw(python ~/git/kernel-tools/codereview/upload.py --git_no_find_copies -y),"-r", $batch_reviewers->{$batch}, "--send_mail", "-m", "structured logging auto-conversion of ".$batch, "HEAD");
}
