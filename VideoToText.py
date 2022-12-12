# Script for transforming a corpus into a list of topics

import re
import sys
import time
import moviepy.editor as mp
import pydub
from pydub import AudioSegment
from pydub.silence import split_on_silence
import os
from pathlib import Path, WindowsPath
import json
import boto3
from botocore.errorfactory import ClientError
import asyncio


def progress_line(i=0, m=100, num=50):
    percentage = i/m
    hash_number = round(percentage*num)
    loadBar = '[' + '#'*hash_number + '-'*(num-hash_number) + ']' + f'{round(percentage*100, 2)}%: {i} of {m}'
    return loadBar


def utf8len(s):
    return len(s.encode('utf-8'))


def chunk_text(text, byteSize=5000):
    text = re.split('\W', text)
    chunks = []
    chunk = ""
    for sentence in text:

        tmp = chunk
        chunk += " " + sentence
        chunk = chunk.strip()

        if utf8len(chunk) >= byteSize:
            chunks.append(tmp)
            chunk = ""
    chunks.append(chunk)
    return chunks


def read_transcript(t):
    with open(t, encoding='utf-8') as text:
        read = text.read()
    return read


class IsNotAFile(Exception):

    def __init__(self, path):
        self.path = path
        super().__init__(path + "isn't a file.")


class VideoToText:

    def __init__(self, config: Path, media_files: str or list, transcripts:  list or dict or Path = None, translate_to: str = None, remove_silence=True):

        pydub.AudioSegment.converter = "C:\\ffmpeg\\bin\\ffmpeg.exe"
        pydub.AudioSegment.ffprobe = "C:\\ffmpeg\\bin\\ffprobe.exe"

        self.remove_silence = remove_silence
        self.config = self.local_json_reader(config)
        self.credentials_path = self.config['credentials']
        self.s3_bucket = self.config['s3_bucket']
        if isinstance(media_files, str):
            self.media_files = [Path(media_files).absolute()]
        else:
            if all([isinstance(m, str) for m in media_files]):
                self.media_files = list(map(Path, media_files))

        if not os.path.isfile(Path(self.credentials_path)):
            raise IsNotAFile(self.credentials_path)

        self.credentials = self.read_credentials()

        self.session = boto3.Session(aws_access_key_id=self.credentials["aws_access_key_id"],
                                     aws_secret_access_key=self.credentials["aws_secret_access_key"],
                                     aws_session_token=self.credentials["aws_session_token"],
                                     region_name='eu-central-1')
        self.s3 = self.session.client('s3')

        # session = boto3.Session(profile_name = 'cdp_staging_developer')

        # transcripts could be: list of texts, list of paths or a path to directory
        if transcripts is not None:
            if os.path.isdir(transcripts):
                self.transcripts = {f: read_transcript(Path(transcripts, f)) for f in os.listdir(transcripts) if os.path.isfile(Path(transcripts, f))}
            elif isinstance(transcripts, list) and all([os.path.isfile(t) for t in transcripts]):
                self.transcripts = {t: read_transcript(t) for t in transcripts}
            elif isinstance(transcripts, list) and all([isinstance(t, str) for t in transcripts]):
                self.transcripts = {i: t for i, t in enumerate(transcripts)}
            elif isinstance(transcripts, dict) and all([isinstance(t, str) for t in transcripts.values()]):
                self.transcripts = transcripts
            else:
                raise f"Transcripts should be: strings, list of strings, dict of strings or path(s). Given: {type(transcripts)}"

            print("Transcripts read successfully")

        if translate_to is not None:
            self.transcripts = {k: self.translate_to_en(t, target_lang=translate_to) for k, t in self.transcripts.items()}
            print(f"Transcripts translated into {translate_to}")

    def does_object_exist(self, key: str) -> bool:
        """
        :param key: key path to the file inside the bucket
        :return: true/false
        """
        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=key)
        except ClientError:
            return False

        return True

    def local_json_reader(self, path: Path) -> dict:
        """
        :param path: key to .json file in the s3 bucket
        :return: dict of the file contents
        """
        if os.path.exists(path) and os.path.isfile(path):
            with open(path) as p:
                contents = p.read()

            contents = json.loads(contents)
        else:
            print(f'File {path} doesn\'t exist.')
            sys.exit()
        return contents

    def aws_json_reader(self, path: str) -> dict:
        """
        :param path: key to .json file in the s3 bucket
        :return: dict of the file contents
        """
        path = Path(path).name
        contents = {}
        if self.does_object_exist(key=path):
            data = self.s3.get_object(Bucket=self.s3_bucket, Key=path)
            contents = data['Body'].read().decode('utf-8')
            contents = json.loads(contents)
        else:
            print(f'File {path} doesn\'t exist.')
            sys.exit(0)
        return contents

    def aws_path_exists(self, key: str):
        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=key)
        except ClientError:
            return False
        return True


    def read_credentials(self) -> dict:
        """
        :param path: path to the credentials
        :return: dict with credentials
        """

        credentials = {"aws_access_key_id": None,
                       "aws_session_token": None,
                       "aws_secret_access_key": None}

        with open(self.credentials_path) as creds:
            for line in creds:
                line = line.split("=")
                if line[0] in credentials.keys():
                    credentials[line[0]] = line[1].strip('\n')

        return credentials

    #async def write_audiofile(self, audio, path):
    #    return audio.write_audiofile(path)

    def make_audios(self):
        for m in self.media_files:
            yield self.make_audio(m)

    def make_audio(self, media_file: Path) -> str:

        print(f'Transforming {media_file}...')
        corrected_name = re.sub("[^a-zA-Z0-9_\-\s\.]", "", media_file.stem.strip("_audio")).replace(' ', '_') + ".mp3"
        aws_audio_path = self.s3_bucket + "/" + corrected_name

        # Local part
        # audio_path = media_file
        new_name = media_file.stem.strip("_audio") + "_audio.mp3"
        audio_path = Path(media_file.parent, new_name) # could be an issue

        if not os.path.exists(audio_path):
            # Reading and splitting the audio file into chunks
            print(f'Reading {media_file}')
            file_format = media_file.suffix.replace('.', '')
            audio = AudioSegment.from_file(media_file, format=file_format)
            if self.remove_silence:
                print('Looking for silence parts...')
                # if audio
                audio_chunks = split_on_silence(audio
                                                , min_silence_len=60000
                                                , silence_thresh=-45
                                                , seek_step=1000
                                                , keep_silence=100
                                                )

                # Putting the file back together
                print('Removing silence...')
                sum_duration = 0
                audio = AudioSegment.empty()
                for chunk in audio_chunks:
                    # print('Sum_duration: ', round(sum_duration/3600*4, 2) * 100)
                    sum_duration += chunk.duration_seconds
                    if sum_duration >= 3600*4:
                        break
                    audio += chunk

                audio.export(audio_path, format="mp3")
                del audio
                print('Done.')
                print(f'Audio saved locally to {audio_path}')

        # AWS part
        if not self.aws_path_exists(corrected_name):
            self.s3.upload_file(str(audio_path), self.s3_bucket, corrected_name)
            print(f'Audio saved to S3://{aws_audio_path}')
        else:
            print(f'Audio is already in the bucket.')

        return aws_audio_path

    async def check_status(self, response, transcriber, transcribe_name):
        # TODO: debug status
        status = str(response['TranscriptionJob']['TranscriptionJobStatus'])

        while status == 'IN_PROGRESS':
            await asyncio.sleep(10)
            print(f"Status {transcribe_name}: ", status)
            # print('Transcription is in progress' + '.'*job_lasted, end='\r')
            status = transcriber.list_transcription_jobs(JobNameContains=transcribe_name)['TranscriptionJobSummaries']
            # print(status.keys())
            if status:
                status = status[0]['TranscriptionJobStatus']

        return status

    async def make_transcript(self, media_file: str, q: asyncio.Queue, numLanguages=1, MediaFormat='mp3') -> None:
        """
        :return: transcript of the record
        """

        audio_path = "s3://" + media_file
        # audio_path = "s3://webinar-recordings/99300643525-Happy_Thyroid_Hour-2_audio.mp3"
        transcribe_name = Path(audio_path).stem + "---" + str(round(time.time()))

        print(f'Making transcript of {audio_path}...')

        # Look for Path(audio_path).stem in the bucket before starting the job!!!
        s3_objects = self.s3.list_objects(Bucket=self.s3_bucket)
        s3_objects = [i['Key'] for i in s3_objects['Contents']]
        found = [i for i in s3_objects if Path(audio_path).stem in i and i.endswith('json')]
        if found:
            print('Transcript already exists!')
            transcript = self.aws_json_reader("S3://" + self.s3_bucket + '/' + found[0])
            lang = transcript['results'].get('language_code')
            transcript = ' '.join([i['transcript'] for i in transcript['results']['transcripts']])
            await q.put((lang, transcript))
            return

        # If the file is new, make new transcript
        IdentifyMultipleLanguages = False
        IdentifyLanguage = True
        if numLanguages > 2:
            IdentifyMultipleLanguages = True
            IdentifyLanguage = False

        print('Creating transcript...')
        transcriber = self.session.client(service_name='transcribe')
        response = transcriber.start_transcription_job(
            TranscriptionJobName=transcribe_name,
            MediaFormat=MediaFormat,
            Media={
                'MediaFileUri': audio_path,
            },
            OutputBucketName=self.s3_bucket,
            OutputKey=transcribe_name + '.json',
            IdentifyLanguage=IdentifyLanguage,
            IdentifyMultipleLanguages=IdentifyMultipleLanguages
        )

        with open(f'{transcribe_name}_report.json', 'w') as r:
            r.write(str(response))

        print('Awaiting results...')
        status = await self.check_status(response, transcriber, transcribe_name)

        print('Job Status: ', status)

        print(f'Transcript: {transcribe_name} has been saved to {self.s3_bucket}.')

        transcript = self.aws_json_reader("S3://" + self.s3_bucket + '/' + transcribe_name + '.json')
        lang = transcript['results'].get('language_code')
        transcript = ' '.join([i['transcript'] for i in transcript['results']['transcripts']])

        await q.put((lang, transcript))

    async def make_translation(self, Text, SouceLanguageCode, TargetLanguageCode):
        translate = self.session.client(service_name='translate')
        return translate.translate_text(Text=Text, SourceLanguageCode=SouceLanguageCode, TargetLanguageCode=TargetLanguageCode)[
            'TranslatedText']

    async def translate_to_en(self, q: asyncio.Queue, source_lang='auto', target_lang='en') -> str:
        print('Translating')
        # text = ' '.join([i['transcript'] for i in transcript['results']['transcripts']])
        while True:
            lang, text = await q.get()
            if lang is not None:
                source_lang = lang
            if utf8len(text) > 5000:
                text = chunk_text(text)
                translation = []
                progress_line(i=0, m=len(text), num=20)
                for idx, t in enumerate(text):
                    if len(t) > 0:
                        translated = await self.make_translation(t, source_lang, target_lang)
                        translation.append(translated)
                        progress_line(i=idx, m=len(text), num=20)
                translation = ' '.join(translation)

            else:
                translation = await self.make_translation(text, source_lang, target_lang)
            print('Translation done.')
            q.task_done()
            return translation

    async def get_transcripts(self):
        """
        Main part
        :return:
        """
        # Step 0: convert transcripts into audios
        #aws_audio_paths = {}
        #for m in self.media_files:
        #    aws_audio_paths[m] = self.make_audio(m)

        #audios = asyncio.gather(*(self.make_audio(m) for m in self.media_files))

        # damit es funktioniert braucht man wahrscheinlich eine Schleife (nicht 2) und self.queue
        queue = asyncio.Queue()

        transcripts = [asyncio.create_task(self.make_transcript(a, queue)) for a in self.make_audios()]

        await asyncio.gather(*transcripts)

        translations = [asyncio.create_task(self.translate_to_en(queue)) for _ in range(queue.qsize())]
        # Step 1: make transcripts

        await queue.join()
        for t in translations:
            print(t)
            t.cancel()
        await asyncio.gather(*translations)

        # Step 4: save transcript locally
        results = {k: v for k, v in zip(self.media_files, translations)}
        for k, v in results.items():
            with open(f'{Path(k).stem}.txt', 'w', encoding='utf-8') as t:
                t.write(v.result())

def main():
    # transcripts = Path('transcripts/')
    directory = 'data\\transcriptions and webinar videos\\videos\\'
    files = [directory + f for f in os.listdir(directory) if f.endswith('.mp3')]
    print('Files: ', files)
    # files = [
    #        '92529664999_Teleheart_in_ART_audio.mp3',
    #        '94587503145_Excellence_Academy_Fs_Oujda_Tanger_audio.mp3']

    # file = '91537550993_MMSERM Workshop Clinical Embryology & Reproductive Geneticsy Webinar_Recording.mp4'
    # files = [directory + f for f in files]

    T = VideoToText(config=Path('config.json'), media_files=files, remove_silence=False)
    asyncio.run(T.get_transcripts())


# TODO: make name validation

if __name__ == '__main__':
    main()
