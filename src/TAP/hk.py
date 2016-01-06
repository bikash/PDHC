import os
import sys
import shutil

def dir_scan(_dir):
   print " ~ Scanning %s..." % (_dir)
   files = []

   for root, subdirs, filenames in os.walk(_dir):
      for subdir in subdirs:
         files.extend(dir_scan(os.path.join(_dir, subdir)))

      for filename in filenames:
         filename = os.path.join(_dir, filename)
         if os.path.isfile(filename):
            files.append(filename)

   return files

def show_help():
   print ""
   print " |-- Hard Killer 1.1"
   print " |"
   print " |-- Secure deletion tool"
   print " |"
   print ""
   print "     hk <file_or_dir>"
   print "     hk <file_or_dir> --no-delete, -nd"
   print ""
   sys.exit()

try:

   if __name__ == "__main__":

      files = []

      try:
         file_or_dir = sys.argv[1]
      except:
         show_help()
         sys.exit()

      try:
         delete = sys.argv[2]
      except:
         delete = False

      if not os.path.isdir(file_or_dir):
         files.append(file_or_dir)
      else:
         files.extend(dir_scan(file_or_dir))

      for file in files:

         file_size = os.stat(file).st_size
         file_obj  = open(file, "wb")

         print ""

         if len(file) > 37:
            print " ~ %s... (%s Bytes) " % (file[0:37], file_size)
         else:
            print " ~ %s (%s Bytes) " % (file.ljust(40, "."), file_size)

         print " ~ Generating data to overwrite the file..."

         data = "\xFF".ljust(file_size, "\xFF")
         print data
         print " ~ Overwriting file..."

         file_obj.write(data)
         file_obj.close()

      if delete == False:
         print ""

         for file in files:
            if len(file) > 50:
               print " ~ Deleting %s..." % (file[0:50])
            else:
               print " ~ Deleting %s" %(file.ljust(53, '.'))
            os.remove(file)

         if os.path.isdir(file_or_dir):
            print " ~ Removing %s..." % (file_or_dir)
            shutil.rmtree(file_or_dir)

except KeyboardInterrupt:
   print "\n\n|-- stoped"

print "\n|-- bye"
